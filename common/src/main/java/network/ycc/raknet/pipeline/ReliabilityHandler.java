package network.ycc.raknet.pipeline;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Attribute;
import io.netty.util.concurrent.ScheduledFuture;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.TransportFeedbackEvent;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.packet.FrameSet;
import network.ycc.raknet.packet.Reliability;
import network.ycc.raknet.pipeline.FlushTickHandler.MissedFlushes;
import network.ycc.raknet.utils.Constants;
import network.ycc.raknet.utils.UINT;

import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import static network.ycc.raknet.utils.SaturatedMath.add;
import static network.ycc.raknet.utils.SaturatedMath.multiply;

/**
 * This handler handles the bulk of reliable (framed) transport.
 */
public class ReliabilityHandler extends ChannelDuplexHandler {

    public static final String NAME = "rn-reliability";

    protected final IntSortedSet nackSet = new IntRBTreeSet(UINT.B3.COMPARATOR);
    protected final IntSortedSet nackRepeatSet = new IntRBTreeSet(UINT.B3.COMPARATOR);
    protected final IntSortedSet ackSet = new IntRBTreeSet(UINT.B3.COMPARATOR);
    protected final IntSortedSet ackRepeatSet = new IntRBTreeSet(UINT.B3.COMPARATOR);
    protected final PriorityQueue<Frame> frameQueue = new PriorityQueue<>(Frame.COMPARATOR);
    protected final Int2ObjectLinkedOpenHashMap<FrameSet> pendingFrameSets = new Int2ObjectLinkedOpenHashMap<>();
    protected final Int2LongOpenHashMap rackRetiredFrameSets = new Int2LongOpenHashMap();
    protected final Int2IntOpenHashMap ptoProbeBytesByFrameSet = new Int2IntOpenHashMap();
    protected final Int2IntOpenHashMap orderedHolProbeBytesByFrameSet = new Int2IntOpenHashMap();
    protected final Int2IntOpenHashMap additionalRecoveryBytesByFrameSet = new Int2IntOpenHashMap();
    protected final Int2LongOpenHashMap additionalRecoveryLastSent = new Int2LongOpenHashMap();
    protected final IntOpenHashSet additionallyRecoveredThisPeriod = new IntOpenHashSet();
    protected final Int2IntOpenHashMap targetedFecChannelsByFrameSet = new Int2IntOpenHashMap();
    protected final ReliableReceiveWindow reliableReceiveWindow = new ReliableReceiveWindow();
    protected final DeferredNackTracker deferredNacks = new DeferredNackTracker();

    protected int queuedBytes = 0;
    protected long inFlightBytes = 0;
    protected long ptoProbeBytesInFlight = 0;
    protected long orderedHolProbeBytesInFlight = 0;
    protected long additionalRecoveryBytesInFlight = 0;

    protected int lastReceivedSeqId = 0;
    protected int nextSendSeqId = 0;
    protected int resendGauge = 0;
    protected int burstTokens = 0;
    protected RakNet.Config config = null; //TODO: not really needed anymore
    protected ChannelHandlerContext ctx;
    protected AdaptiveTransportController adaptive;
    protected boolean pacingScheduled;
    protected boolean coalesceScheduled;
    protected boolean productionScheduled;
    protected boolean removed;
    protected int coalescedFrames;
    protected long coalesceStartedNanos;
    protected ScheduledFuture<?> pacingFuture;
    protected ScheduledFuture<?> coalesceFuture;
    protected ScheduledFuture<?> ackFlushFuture;
    protected ScheduledFuture<?> deferredNackFuture;
    protected long deferredNackDeadlineNanos = Long.MAX_VALUE;
    protected ScheduledFuture<?> nackRepeatFuture;
    protected ScheduledFuture<?> ackRepeatFuture;
    protected ScheduledFuture<?> rackLossFuture;
    protected long rackLossDeadlineNanos = Long.MAX_VALUE;
    protected long latestAckedSentTimeNanos;
    protected int rackReorderingMultiplierEighths = 10;
    protected int rackCleanAckCount;
    protected ScheduledFuture<?> ptoFuture;
    protected long ptoDeadlineNanos = Long.MAX_VALUE;
    protected long lastAckProgressNanos;
    protected long lastPtoProbeSentNanos;
    protected int ptoCount;
    protected long lastOrderedHolProbeSentNanos;
    protected int lastOrderedHolProbeChannel = -1;
    protected int lastOrderedHolProbeOrderIndex = -1;
    protected boolean applicationLimitedRecoveryPeriod;
    protected long targetedFecWindowStartedNanos;
    protected int targetedFecBytesInWindow;
    protected final AdaptiveNackGrace adaptiveNackGrace = new AdaptiveNackGrace();
    protected final AdaptiveAckProtection adaptiveAckProtection = new AdaptiveAckProtection();

    // Item 3: track when first ACK was queued for time-based flush
    protected long firstAckNanos = 0;
    private static final long ACK_FLUSH_DELAY_NANOS = 2_000_000; // 2ms
    private static final long MIN_NACK_REORDER_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(4);
    private static final long MAX_NACK_REORDER_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(25);
    private static final long MIN_NACK_REPEAT_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
    private static final long MAX_NACK_REPEAT_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(20);
    private static final int MAX_DEFERRED_NACK_GAP = 2;
    private static final long RACK_TIMER_GRANULARITY_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final int RACK_BASE_REORDERING_MULTIPLIER_EIGHTHS = 10; // 1.25 RTT
    private static final int RACK_MAX_REORDERING_MULTIPLIER_EIGHTHS = 16; // 2 RTT
    private static final int RACK_CLEAN_ACKS_TO_DECAY = 64;
    private static final long MIN_RACK_RETIRED_RETENTION_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final long PTO_TIMER_GRANULARITY_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final int MAX_PTO_BACKOFF_EXPONENT = 6;
    private static final long MIN_ORDERED_HOL_PROBE_AGE_NANOS = TimeUnit.MILLISECONDS.toNanos(250);
    private static final long MIN_ORDERED_HOL_PROBE_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final long REMOTE_ORDERED_HOL_RETENTION_NANOS = TimeUnit.SECONDS.toNanos(3);
    private static final boolean ADAPTIVE_NACK_GRACE_ENABLED = Boolean.parseBoolean(
            System.getProperty("raknetify.adaptiveNackGrace", "true"));
    private static final boolean ADAPTIVE_NACK_PROTECTION_ENABLED = Boolean.parseBoolean(
            System.getProperty("raknetify.adaptiveNackProtection", "true"));
    private static final boolean ADAPTIVE_ACK_PROTECTION_ENABLED = Boolean.parseBoolean(
            System.getProperty("raknetify.adaptiveAckProtection", "true"));
    private static final boolean RACK_LOSS_DETECTION_ENABLED = Boolean.parseBoolean(
            System.getProperty("raknetify.rackLossDetection", "true"));
    private static final boolean PTO_PROBES_ENABLED = Boolean.parseBoolean(
            System.getProperty("raknetify.ptoProbes", "true"));
    private static final boolean REMOTE_ORDERED_HOL_RECOVERY_ENABLED = Boolean.parseBoolean(
            System.getProperty("raknetify.remoteOrderedHolRecovery", "true"));
    private static final boolean APPLICATION_LIMITED_RECOVERY_ENABLED = Boolean.parseBoolean(
            System.getProperty("raknetify.applicationLimitedRecovery", "true"));
    private static final boolean TARGETED_FEC_ENABLED = Boolean.parseBoolean(
            System.getProperty("raknetify.targetedFec", "true"));
    private static final double TARGETED_FEC_MIN_DEBT = 2.0D;
    // Item 7: guard against recursive flush from recallFrameSet
    protected boolean flushing = false;

    public void onNegotiatedMtu(int mtu) {
        if (adaptive != null) adaptive.onNegotiatedMtu(mtu);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        removed = false;
        config = RakNet.config(ctx);
        this.ctx = ctx;
        this.adaptive = new AdaptiveTransportController(config);
        ctx.channel().attr(RakNet.WRITABLE).set(true);
        if (config.isIgnoreResendGauge()) this.resendGauge = 2;
        rackRetiredFrameSets.defaultReturnValue(Long.MIN_VALUE);
        ptoProbeBytesByFrameSet.defaultReturnValue(0);
        orderedHolProbeBytesByFrameSet.defaultReturnValue(0);
        additionalRecoveryBytesByFrameSet.defaultReturnValue(0);
        additionalRecoveryLastSent.defaultReturnValue(Long.MIN_VALUE);
        targetedFecChannelsByFrameSet.defaultReturnValue(-1);
        publishRecoveryPolicies(System.nanoTime());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        removed = true;
        cancelTransportTasks();
        cancelDeferredNackTask();
        cancelNackRepeatTask();
        cancelAckRepeatTask();
        cancelRackLossTask();
        cancelPtoTask();
        deferredNacks.clear();
        nackSet.clear();
        nackRepeatSet.clear();
        ackSet.clear();
        ackRepeatSet.clear();
        rackRetiredFrameSets.clear();
        ptoProbeBytesByFrameSet.clear();
        orderedHolProbeBytesByFrameSet.clear();
        additionalRecoveryBytesByFrameSet.clear();
        additionalRecoveryLastSent.clear();
        additionallyRecoveredThisPeriod.clear();
        targetedFecChannelsByFrameSet.clear();
        firstAckNanos = 0L;
        coalescedFrames = 0;
        clearQueue(null);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof Frame) {
            final Frame frame = (Frame) msg;
            // Item 1: detect idle→active transition for immediate flush.
            // Only flush on the first frame after silence — subsequent frames
            // are batched by the normal flush cycle for packet efficiency.
            final boolean wasIdle = frameQueue.isEmpty();
            adaptive.onApplicationQueued(System.nanoTime(), frame.getRoughPacketSize());
            queueFrame(frame);
            frame.setPromise(promise);
            Constants.packetLossCheck(pendingFrameSets.size(), "unconfirmed sent packets");
            FlushTickHandler.checkFlushTick(ctx.channel());
            if (wasIdle && frame.getRoughPacketSize() >= config.getMTU() / 2) {
                flush(ctx);
            } else if (wasIdle && !coalesceScheduled) {
                coalesceScheduled = true;
                coalescedFrames = 1;
                coalesceStartedNanos = System.nanoTime();
                coalesceFuture = ctx.executor().schedule(() -> {
                    coalesceFuture = null;
                    coalesceScheduled = false;
                    if (!removed && ctx.channel().isOpen() && !frameQueue.isEmpty()) {
                        config.getMetrics().smallWriteBatch(coalescedFrames,
                                System.nanoTime() - coalesceStartedNanos);
                        flush(ctx);
                    }
                    coalescedFrames = 0;
                }, adaptive.smallWriteCoalesceMicros(), TimeUnit.MICROSECONDS);
            } else if (coalesceScheduled) {
                coalescedFrames++;
            }
        } else {
            ctx.write(msg, promise);
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        if (!ctx.channel().isOpen()) {
            ctx.flush();
            return;
        }
        // Item 7: prevent recursive flush from recallFrameSet during expire path
        if (flushing) return;
        flushing = true;
        try {
            //all data sent in order of priority
            sendResponses(ctx);
            recallExpiredFrameSets();
            produceFrameSets(ctx);
            updateBurstTokens(1);
            updateBackPressure(ctx);
            Constants.packetLossCheck(pendingFrameSets.size(), "resend queue");
            ctx.flush();
        } finally {
            flushing = false;
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        //missed some flush ticks, lets catch up on a few things
        if (evt instanceof MissedFlushes) {
            updateBurstTokens(((MissedFlushes) evt).nFlushes);
        } else if (evt instanceof TransportFeedbackEvent) {
            final TransportFeedbackEvent feedback = (TransportFeedbackEvent) evt;
            if (feedback.getType() == TransportFeedbackEvent.Type.ECN_CE) {
                adaptive.onEcnCe();
            } else if (feedback.getType() == TransportFeedbackEvent.Type.PACKET_TOO_BIG) {
                adaptive.onPacketTooBig(feedback.getMtu(), config.getMTU());
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            ctx.channel().attr(RakNet.LAST_INBOUND_NANOS).set(System.nanoTime());
            if (msg instanceof Reliability.ACK) {
                readAck((Reliability.ACK) msg);
            } else if (msg instanceof Reliability.NACK) {
                readNack((Reliability.NACK) msg);
            } else if (msg instanceof FrameSet) {
                readFrameSet(ctx, (FrameSet) msg);
            } else {
                ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    protected void clearQueue(Throwable t) {
        if (t != null) {
            frameQueue.forEach(frame -> {
                if (frame.getPromise() != null) {
                    frame.getPromise().tryFailure(t);
                }
            });
            pendingFrameSets.values().forEach(set -> set.fail(t));
        }
        frameQueue.forEach(Frame::release);
        frameQueue.clear();
        this.queuedBytes = 0;
        this.inFlightBytes = 0;
        this.ptoProbeBytesInFlight = 0;
        this.orderedHolProbeBytesInFlight = 0;
        this.additionalRecoveryBytesInFlight = 0;
        cancelPtoTask();
        ptoProbeBytesByFrameSet.clear();
        orderedHolProbeBytesByFrameSet.clear();
        additionalRecoveryBytesByFrameSet.clear();
        additionalRecoveryLastSent.clear();
        additionallyRecoveredThisPeriod.clear();
        targetedFecChannelsByFrameSet.clear();
        applicationLimitedRecoveryPeriod = false;
        pendingFrameSets.values().forEach(FrameSet::release);
        pendingFrameSets.clear();
    }

    protected void readFrameSet(ChannelHandlerContext ctx, FrameSet frameSet) {
        final int packetSeqId = frameSet.getSeqId();
        final int forwardDistance = UINT.B3.minusWrap(packetSeqId, lastReceivedSeqId);
        if (config.isNACKEnabled() && forwardDistance > 0) {
            Constants.packetLossCheck(forwardDistance - 1, "incoming sequence gap");
        }
        // Item 3: track first ACK time for time-based flush trigger
        if (ackSet.isEmpty()) {
            firstAckNanos = System.nanoTime();
            // Schedule guaranteed ACK flush so low-traffic connections
            // don't wait until the 50ms flush tick or next FrameSet arrival.
            ackFlushFuture = ctx.channel().eventLoop().schedule(() -> {
                ackFlushFuture = null;
                if (!removed && ctx.channel().isOpen()) trySendResponses(ctx);
            }, ACK_FLUSH_DELAY_NANOS + 500_000L, TimeUnit.NANOSECONDS);
        }
        ackSet.add(packetSeqId);
        if (config.isNACKEnabled()) {
            nackSet.remove(packetSeqId);
            nackRepeatSet.remove(packetSeqId);
            if (nackRepeatSet.isEmpty()) cancelNackRepeatTask();
            if (deferredNacks.cancel(packetSeqId)) {
                config.getMetrics().reorderedPacket(1);
                adaptiveNackGrace.onReordered(System.nanoTime());
            }
        }
        if (forwardDistance > 0) {
            final int missingCount = forwardDistance - 1;
            final long now = System.nanoTime();
            final boolean graceEligible = config.isNACKEnabled() && shouldDeferNackGap(missingCount);
            final boolean deferGap = graceEligible && (!ADAPTIVE_NACK_GRACE_ENABLED
                    || adaptiveNackGrace.shouldDefer(now));
            final boolean bypassGrace = graceEligible && !deferGap;
            if (config.isNACKEnabled() && missingCount > MAX_DEFERRED_NACK_GAP) {
                confirmDeferredNacks();
            }
            if (!config.isNACKEnabled()) {
                lastReceivedSeqId = packetSeqId;
            } else {
                lastReceivedSeqId = UINT.B3.plus(lastReceivedSeqId, 1);
                while (lastReceivedSeqId != packetSeqId) { //nack any missed packets before this one
                    if (deferGap) deferNack(ctx, lastReceivedSeqId);
                    else {
                        nackSet.add(lastReceivedSeqId);
                        if (bypassGrace) config.getMetrics().nackGraceBypassed(1);
                    }
                    lastReceivedSeqId = UINT.B3.plus(lastReceivedSeqId, 1);
                }
            }
        }
        config.getMetrics().packetsIn(1);
        config.getMetrics().framesIn(frameSet.getNumPackets());
        final boolean[] duplicateReliableFrameSet = {false};
        frameSet.createFrames(frame -> {
            if (frame.getReliability().isReliable
                    && !reliableReceiveWindow.accept(frame.getReliableIndex())) {
                // Retransmitted FrameSets use a fresh sequence ID, so ACKing the
                // FrameSet is not enough to suppress duplicate reliable frames.
                // In particular, a duplicate split fragment can otherwise create
                // an orphan FrameJoiner builder after the original was completed.
                config.getMetrics().reliableFrameDuplicate(1);
                duplicateReliableFrameSet[0] = true;
                frame.release();
                return;
            }
            ctx.fireChannelRead(frame);
        });
        if (ADAPTIVE_ACK_PROTECTION_ENABLED && duplicateReliableFrameSet[0]) {
            // Count retransmitted FrameSets, not the number of frames inside one
            // datagram. A large packet must not activate protection by itself.
            adaptiveAckProtection.onDuplicateFrameSet(System.nanoTime(), config.getRTTNanos());
        }
        trySendResponses(ctx);
        ctx.fireChannelReadComplete();
    }

    protected void readAck(Reliability.ACK ack) {
        int ackdBytes = 0;
        int nIterations = 0;
        boolean ackProgress = false;
        final long now = System.nanoTime();
        for (Reliability.REntry entry : ack.getEntries()) {
            nIterations += entry.size();
            Constants.packetLossCheck(nIterations, "ack confirm ranges");
            final int max = UINT.B3.plus(entry.idFinish, 1);
            for (int id = entry.idStart; id != max; id = UINT.B3.plus(id, 1)) {
                final FrameSet frameSet = pendingFrameSets.remove(id);
                if (frameSet != null) {
                    targetedFecChannelsByFrameSet.remove(id);
                    ackProgress = true;
                    final int acknowledgedProbeBytes = clearPtoProbeTracking(id);
                    if (acknowledgedProbeBytes > 0) {
                        config.getMetrics().ptoProbeAcked(acknowledgedProbeBytes);
                    }
                    final int acknowledgedHolProbeBytes = clearOrderedHolProbeTracking(id);
                    if (acknowledgedHolProbeBytes > 0) {
                        config.getMetrics().orderedHolProbeAcked(acknowledgedHolProbeBytes);
                    }
                    clearAdditionalRecoveryTracking(id);
                    clearLogicalRecoveryTracking(frameSet);
                    latestAckedSentTimeNanos = Math.max(latestAckedSentTimeNanos, frameSet.getSentTime());
                    onRackCleanAck();
                    ackdBytes += frameSet.getRoughSize();
                    inFlightBytes = Math.max(0, inFlightBytes - frameSet.getRoughSize());
                    final long remainingInFlight = totalInFlightBytes();
                    final boolean applicationLimited = frameQueue.isEmpty() && queuedBytes == 0
                            && remainingInFlight < adaptive.congestionWindowBytes() / 2L;
                    adaptive.onAck(frameSet.getRoughSize(), System.nanoTime() - frameSet.getSentTime(),
                            remainingInFlight, applicationLimited, frameSet.getSentTime());
                    adjustResendGauge(1);
                    frameSet.succeed();
                    frameSet.release();
                    tryProduceFrameSets();
                } else if (rackRetiredFrameSets.containsKey(id)) {
                    rackRetiredFrameSets.remove(id);
                    config.getMetrics().rackSpuriousAck(1);
                    onRackSpuriousAck();
                }
            }
        }
        config.getMetrics().bytesACKd(ackdBytes);
        if (ackProgress) {
            // A useful ACK restarts the connection-level PTO. Keeping an older
            // scheduled deadline causes stale probes to fire despite continuous
            // ACK progress because schedulePto deliberately preserves the
            // earliest existing deadline.
            cancelPtoTask();
            lastAckProgressNanos = now;
            lastPtoProbeSentNanos = 0L;
            ptoCount = 0;
        }
        pruneRackRetired(now);
        if (ackProgress) detectRackLosses(now);
        else refreshRackLossTimer(now);
        refreshPtoTimer(now);
    }

    protected void readNack(Reliability.NACK nack) {
        if (!config.isNACKEnabled()) return;

        int bytesNACKd = 0;
        int nIterations = 0;
        for (Reliability.REntry entry : nack.getEntries()) {
            nIterations += entry.size();
            Constants.packetLossCheck(nIterations, "nack confirm ranges");
            final int max = UINT.B3.plus(entry.idFinish, 1);
            for (int id = entry.idStart; id != max; id = UINT.B3.plus(id, 1)) {
                final FrameSet frameSet = pendingFrameSets.remove(id);
                if (frameSet != null) {
                    targetedFecChannelsByFrameSet.remove(id);
                    clearPtoProbeTracking(id);
                    clearOrderedHolProbeTracking(id);
                    clearAdditionalRecoveryTracking(id);
                    bytesNACKd += frameSet.getRoughSize();
                    inFlightBytes = Math.max(0, inFlightBytes - frameSet.getRoughSize());
                    adaptive.onLoss(frameSet.getRoughSize(), false);
                    recallFrameSet(frameSet);
                }
            }
        }
        config.getMetrics().bytesNACKd(bytesNACKd);
        config.getMetrics().nackRetransmit(bytesNACKd);
        refreshRackLossTimer(System.nanoTime());
        refreshPtoTimer(System.nanoTime());
    }

    protected void queueFrame(Frame frame) {
        final int roughPacketSize = frame.getRoughPacketSize();
        if (roughPacketSize > config.getMTU()) {
            throw new CorruptedFrameException(
                    "Finished frame larger than the MTU by " + (roughPacketSize - config
                            .getMTU()));
        }
        frameQueue.add(frame);
        this.queuedBytes += roughPacketSize;
        config.getMetrics().currentQueuedBytes(this.queuedBytes);
    }

    protected void adjustResendGauge(int n) {
        if (config.isIgnoreResendGauge()) {
            this.resendGauge = 2;
            return;
        }
        //clamped gauge, can rebound more easily
        resendGauge = Math.max(
                -config.getDefaultPendingFrameSets(),
                Math.min(config.getDefaultPendingFrameSets(), resendGauge + n)
        );
    }

    protected void updateBurstTokens(int nTicks) {
        if (config.isNoDelayEnabled()) {
            burstTokens = Math.max(0, config.getMaxPendingFrameSets());
        } else {
            // gradual increment or decrement for burst tokens, unless unused
            final boolean burstUnused = pendingFrameSets.size() < burstTokens / 2;
            if (resendGauge > 1 && !burstUnused) {
                burstTokens += 1 * nTicks;
            } else if (resendGauge < -1 || burstUnused) {
                burstTokens -= 3 * nTicks;
            }
            burstTokens = Math.max(Math.min(burstTokens, config.getMaxPendingFrameSets()), 0);
        }
        config.getMetrics().measureBurstTokens(burstTokens);
    }

    protected void sendResponses(ChannelHandlerContext ctx) {
        final long now = System.nanoTime();
        publishRecoveryPolicies(now);
        if (!ackSet.isEmpty()) {
            ctx.write(new Reliability.ACK(ackSet)).addListener(RakNet.INTERNAL_WRITE_LISTENER);
            config.getMetrics().acksSent(ackSet.size());
            if (ADAPTIVE_ACK_PROTECTION_ENABLED && adaptiveAckProtection.isActive(now)) {
                ackRepeatSet.addAll(ackSet);
                scheduleAckRepeat(ctx, adaptiveAckProtection.repeatDelayNanos(config.getRTTNanos()));
            }
            ackSet.clear();
            firstAckNanos = 0;
            cancelAckFlushTask();
        }
        if (config.isNACKEnabled() && !nackSet.isEmpty() && config.isAutoRead()) { //only nack if we can read
            ctx.write(new Reliability.NACK(nackSet)).addListener(RakNet.INTERNAL_WRITE_LISTENER);
            config.getMetrics().nacksSent(nackSet.size());
            if (isNackRepeatActive(now)) {
                nackRepeatSet.addAll(nackSet);
                scheduleNackRepeat(ctx, nackRepeatDelayNanos(config.getRTTNanos()));
            }
            nackSet.clear();
        }
    }

    protected void trySendResponses(ChannelHandlerContext ctx) {
        // Item 3: send ACKs on count OR time threshold (2ms), whichever comes first
        final boolean countTrigger = ackSet.size() >= config.getDefaultPendingFrameSets() - 1;
        final boolean timeTrigger = !ackSet.isEmpty()
                && System.nanoTime() - firstAckNanos > ACK_FLUSH_DELAY_NANOS;
        if (countTrigger || timeTrigger) {
            sendResponses(ctx);
            ctx.flush();
        }
    }

    protected void recallExpiredFrameSets() {
        final ObjectIterator<FrameSet> packetItr = pendingFrameSets.values().iterator();
        final long now = System.nanoTime();
        while (packetItr.hasNext()) {
            final FrameSet frameSet = packetItr.next();
            // exponential backoff: 1x, 2x, 4x, 8x (max)
            final long timeout = retransmissionTimeoutNanos(
                    config.getRTTNanos(), config.getRTTStdDevNanos(), config.getRetryDelayNanos(),
                    frameSet.getRetryCount());
            if (now - frameSet.getSentTime() > timeout) {
                packetItr.remove();
                targetedFecChannelsByFrameSet.remove(frameSet.getSeqId());
                clearPtoProbeTracking(frameSet.getSeqId());
                clearOrderedHolProbeTracking(frameSet.getSeqId());
                clearAdditionalRecoveryTracking(frameSet.getSeqId());
                inFlightBytes = Math.max(0, inFlightBytes - frameSet.getRoughSize());
                adaptive.onLoss(frameSet.getRoughSize(), true);
                config.getMetrics().timeoutRetransmit(frameSet.getRoughSize());
                recallFrameSet(frameSet);
                continue;
            }
            // remaining FrameSets are newer — check sentTime ordering still holds
            // since higher retryCount = longer timeout, an older FrameSet with high
            // retryCount could be before a newer FrameSet with low retryCount.
            // Continue scanning rather than breaking to handle this correctly.
        }
        refreshRackLossTimer(now);
        refreshPtoTimer(now);
    }

    static long rackReorderingWindowNanos(long rttNanos, int multiplierEighths) {
        final long rtt = Math.max(1L, rttNanos);
        final int multiplier = Math.max(RACK_BASE_REORDERING_MULTIPLIER_EIGHTHS,
                Math.min(RACK_MAX_REORDERING_MULTIPLIER_EIGHTHS, multiplierEighths));
        return Math.max(RACK_TIMER_GRANULARITY_NANOS,
                multiply(rtt, multiplier) / 8L);
    }

    /**
     * Uses ACK progress from a chronologically newer transmission to infer that
     * older outstanding FrameSets were lost. Unlike the fallback RTO this path
     * deliberately ignores retryCount: a retransmission can itself be proven lost.
     */
    protected void detectRackLosses(long now) {
        cancelRackLossTask();
        if (!RACK_LOSS_DETECTION_ENABLED || latestAckedSentTimeNanos == 0L
                || pendingFrameSets.isEmpty()) {
            return;
        }

        final long reorderingWindow = rackReorderingWindowNanos(
                config.getRTTNanos(), rackReorderingMultiplierEighths);
        final ArrayList<FrameSet> lost = new ArrayList<>();
        long nextDeadline = Long.MAX_VALUE;
        final ObjectIterator<FrameSet> iterator = pendingFrameSets.values().iterator();
        while (iterator.hasNext()) {
            final FrameSet frameSet = iterator.next();
            if (frameSet.getSentTime() >= latestAckedSentTimeNanos) continue;
            final long deadline = add(frameSet.getSentTime(), reorderingWindow);
            if (now >= deadline) {
                iterator.remove();
                targetedFecChannelsByFrameSet.remove(frameSet.getSeqId());
                clearPtoProbeTracking(frameSet.getSeqId());
                clearOrderedHolProbeTracking(frameSet.getSeqId());
                clearAdditionalRecoveryTracking(frameSet.getSeqId());
                inFlightBytes = Math.max(0L, inFlightBytes - frameSet.getRoughSize());
                adaptive.onLoss(frameSet.getRoughSize(), false);
                config.getMetrics().rackRetransmit(frameSet.getRoughSize());
                rackRetiredFrameSets.put(frameSet.getSeqId(), add(now,
                        Math.max(MIN_RACK_RETIRED_RETENTION_NANOS,
                                multiply(Math.max(1L, config.getRTTNanos()), 4L))));
                lost.add(frameSet);
            } else {
                nextDeadline = Math.min(nextDeadline, deadline);
            }
        }

        for (FrameSet frameSet : lost) recallFrameSet(frameSet);
        pruneRackRetired(now);
        if (nextDeadline != Long.MAX_VALUE) scheduleRackLossCheck(nextDeadline);
        refreshPtoTimer(now);
    }

    static long ptoTimeoutNanos(long rttNanos, long rttStdDevNanos,
                                long retryDelayNanos, int ptoCount) {
        final long rtt = Math.max(1L, rttNanos);
        final long variation = multiply(Math.max(0L, rttStdDevNanos), 4L);
        final long base = add(add(rtt,
                Math.max(PTO_TIMER_GRANULARITY_NANOS, variation)),
                Math.max(0L, retryDelayNanos));
        return multiply(Math.max(PTO_TIMER_GRANULARITY_NANOS, base),
                1L << Math.min(Math.max(0, ptoCount), MAX_PTO_BACKOFF_EXPONENT));
    }

    protected void refreshPtoTimer(long now) {
        if (!PTO_PROBES_ENABLED) {
            cancelPtoTask();
            return;
        }
        final FrameSet candidate = selectPtoProbeCandidate();
        if (candidate == null) {
            cancelPtoTask();
            ptoCount = 0;
            lastPtoProbeSentNanos = 0L;
            publishPtoState(now);
            return;
        }
        final long anchor = Math.max(candidate.getSentTime(),
                Math.max(lastAckProgressNanos, lastPtoProbeSentNanos));
        final long deadline = add(anchor, ptoTimeoutNanos(
                config.getRTTNanos(), config.getRTTStdDevNanos(),
                config.getRetryDelayNanos(), ptoCount));
        if (deadline <= now) sendPtoProbe(now);
        else schedulePto(deadline);
        publishPtoState(now);
    }

    protected FrameSet selectPtoProbeCandidate() {
        final RakNet.OrderedHolFeedback feedback = remoteOrderedHolFeedback(System.nanoTime());
        final FrameSet directed = selectRemoteOrderedHolCandidate(feedback, false, 0L, 0L);
        if (directed != null) return directed;
        FrameSet oldestReliable = null;
        FrameSet oldestOrdered = null;
        FrameSet highestDebtOrdered = null;
        for (FrameSet frameSet : pendingFrameSets.values()) {
            if (!frameSet.hasReliableFrame()) continue;
            if (oldestReliable == null || frameSet.getSentTime() < oldestReliable.getSentTime()) {
                oldestReliable = frameSet;
            }
            if (frameSet.hasReliableOrderedFrame()
                    && (oldestOrdered == null || frameSet.getSentTime() < oldestOrdered.getSentTime())) {
                oldestOrdered = frameSet;
            }
            if (frameSet.hasRetriedReliableOrderedFrame()
                    && (highestDebtOrdered == null
                    || frameSet.maximumRetryCount() > highestDebtOrdered.maximumRetryCount()
                    || (frameSet.maximumRetryCount() == highestDebtOrdered.maximumRetryCount()
                    && frameSet.getSentTime() < highestDebtOrdered.getSentTime()))) {
                highestDebtOrdered = frameSet;
            }
        }
        if (highestDebtOrdered != null) return highestDebtOrdered;
        return oldestOrdered != null ? oldestOrdered : oldestReliable;
    }

    /**
     * Consumes optional peer HOL diagnostics without changing the RakNet wire
     * format. One exact same-sequence probe per feedback interval is enough to
     * target the blocked ordering index while remaining bounded under BULK.
     */
    public void onRemoteOrderedHolFeedback() {
        if (!REMOTE_ORDERED_HOL_RECOVERY_ENABLED || ctx == null || !ctx.channel().isOpen()) return;
        final long now = System.nanoTime();
        final RakNet.OrderedHolFeedback feedback = remoteOrderedHolFeedback(now);
        if (feedback == null) return;
        final long rtt = Math.max(PTO_TIMER_GRANULARITY_NANOS, config.getRTTNanos());
        if (feedback.ageNanos < Math.max(MIN_ORDERED_HOL_PROBE_AGE_NANOS,
                multiply(rtt, 2L))) return;
        final boolean sameTarget = feedback.channel == lastOrderedHolProbeChannel
                && feedback.blockedOrderIndex == lastOrderedHolProbeOrderIndex;
        final long interval = Math.max(MIN_ORDERED_HOL_PROBE_INTERVAL_NANOS, rtt);
        if (sameTarget && now - lastOrderedHolProbeSentNanos < interval) return;

        final FrameSet candidate = selectRemoteOrderedHolCandidate(feedback, false, now, rtt);
        if (candidate == null) {
            tryProduceFrameSets();
            return;
        }
        final int bytes = candidate.getRoughSize();
        if (adaptive.sendBudget(now, totalInFlightBytes(), bytes, queuedBytes) <= 0) return;

        candidate.touch("Remote ordered HOL probe");
        ctx.write(candidate.retain()).addListener(RakNet.INTERNAL_WRITE_LISTENER);
        adaptive.onPacingBatchSent(1);
        orderedHolProbeBytesByFrameSet.addTo(candidate.getSeqId(), bytes);
        orderedHolProbeBytesInFlight += bytes;
        config.getMetrics().orderedHolProbe(feedback.channel, bytes);
        config.getMetrics().packetsOut(1);
        config.getMetrics().framesOut(candidate.getNumPackets());
        lastOrderedHolProbeSentNanos = now;
        lastOrderedHolProbeChannel = feedback.channel;
        lastOrderedHolProbeOrderIndex = feedback.blockedOrderIndex;
        lastPtoProbeSentNanos = now; // share speculative-send pacing with PTO
        ctx.flush();
        refreshPtoTimer(now);
    }

    private RakNet.OrderedHolFeedback remoteOrderedHolFeedback(long now) {
        if (!REMOTE_ORDERED_HOL_RECOVERY_ENABLED || ctx == null) return null;
        final Attribute<RakNet.OrderedHolFeedback> attribute =
                ctx.channel().attr(RakNet.REMOTE_ORDERED_HOL);
        if (attribute == null) return null;
        final RakNet.OrderedHolFeedback feedback = attribute.get();
        if (feedback == null || feedback.channel < 0 || feedback.channel >= 8
                || feedback.blockedOrderIndex < 0
                || now - feedback.receivedAtNanos > REMOTE_ORDERED_HOL_RETENTION_NANOS) return null;
        return feedback;
    }

    private FrameSet selectRemoteOrderedHolCandidate(RakNet.OrderedHolFeedback feedback,
                                                      boolean additionalRecoveryOnly,
                                                      long now, long cooldownNanos) {
        if (feedback == null) return null;
        FrameSet best = null;
        for (FrameSet frameSet : pendingFrameSets.values()) {
            if (!frameSet.containsReliableOrderedFrame(
                    feedback.channel, feedback.blockedOrderIndex)) continue;
            if (additionalRecoveryOnly
                    && !isAdditionalRecoveryEligible(frameSet, now, cooldownNanos)) continue;
            if (best == null || frameSet.maximumRetryCount() > best.maximumRetryCount()
                    || (frameSet.maximumRetryCount() == best.maximumRetryCount()
                    && frameSet.getSentTime() < best.getSentTime())) best = frameSet;
        }
        return best;
    }

    boolean isRemoteOrderedHolTarget(int sequenceId, long now) {
        final FrameSet frameSet = pendingFrameSets.get(sequenceId);
        final RakNet.OrderedHolFeedback feedback = remoteOrderedHolFeedback(now);
        return frameSet != null && feedback != null && frameSet.containsReliableOrderedFrame(
                feedback.channel, feedback.blockedOrderIndex);
    }

    protected void sendPtoProbe(long now) {
        cancelPtoTask();
        final FrameSet candidate = selectPtoProbeCandidate();
        if (candidate == null || ctx == null || !ctx.channel().isOpen()) return;
        final int bytes = candidate.getRoughSize();
        if (adaptive.sendBudget(now, totalInFlightBytes(), bytes, queuedBytes) <= 0) {
            schedulePto(add(now, Math.max(PTO_TIMER_GRANULARITY_NANOS,
                    adaptive.nanosUntilSend(now))));
            return;
        }

        candidate.touch("PTO probe");
        ctx.write(candidate.retain()).addListener(RakNet.INTERNAL_WRITE_LISTENER);
        adaptive.onPacingBatchSent(1);
        ptoProbeBytesByFrameSet.addTo(candidate.getSeqId(), bytes);
        ptoProbeBytesInFlight += bytes;
        config.getMetrics().ptoProbe(bytes);
        config.getMetrics().packetsOut(1);
        config.getMetrics().framesOut(candidate.getNumPackets());
        lastPtoProbeSentNanos = now;
        ptoCount = Math.min(MAX_PTO_BACKOFF_EXPONENT, ptoCount + 1);
        publishPtoState(now);
        ctx.flush();
        refreshPtoTimer(now);
    }

    private void schedulePto(long deadlineNanos) {
        if (ctx == null || !ctx.channel().isOpen()) return;
        if (ptoFuture != null && !ptoFuture.isDone() && ptoDeadlineNanos <= deadlineNanos) return;
        cancelPtoTask();
        ptoDeadlineNanos = deadlineNanos;
        ptoFuture = ctx.executor().schedule(() -> {
            ptoFuture = null;
            ptoDeadlineNanos = Long.MAX_VALUE;
            if (ctx.channel().isOpen()) sendPtoProbe(System.nanoTime());
        }, Math.max(0L, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
    }

    private void cancelPtoTask() {
        if (ptoFuture != null) ptoFuture.cancel(false);
        ptoFuture = null;
        ptoDeadlineNanos = Long.MAX_VALUE;
    }

    private int clearPtoProbeTracking(int sequenceId) {
        final int bytes = ptoProbeBytesByFrameSet.remove(sequenceId);
        if (bytes > 0) ptoProbeBytesInFlight = Math.max(0L, ptoProbeBytesInFlight - bytes);
        return bytes;
    }

    private int clearOrderedHolProbeTracking(int sequenceId) {
        final int bytes = orderedHolProbeBytesByFrameSet.remove(sequenceId);
        if (bytes > 0) {
            orderedHolProbeBytesInFlight = Math.max(0L, orderedHolProbeBytesInFlight - bytes);
        }
        return bytes;
    }

    private int clearAdditionalRecoveryTracking(int sequenceId) {
        final int bytes = additionalRecoveryBytesByFrameSet.remove(sequenceId);
        if (bytes > 0) {
            additionalRecoveryBytesInFlight = Math.max(0L,
                    additionalRecoveryBytesInFlight - bytes);
        }
        return bytes;
    }

    private void clearLogicalRecoveryTracking(FrameSet frameSet) {
        frameSet.forEachReliableIndex(index -> {
            additionalRecoveryLastSent.remove(index);
            additionallyRecoveredThisPeriod.remove(index);
        });
    }

    private long totalInFlightBytes() {
        return add(add(add(inFlightBytes, ptoProbeBytesInFlight),
                orderedHolProbeBytesInFlight), additionalRecoveryBytesInFlight);
    }

    private void publishPtoState(long now) {
        config.getMetrics().ptoState(ptoCount,
                lastAckProgressNanos == 0L ? 0L : Math.max(0L, now - lastAckProgressNanos));
    }

    protected void updateApplicationLimitedRecovery(long now) {
        final boolean applicationLimited = frameQueue.isEmpty() && queuedBytes == 0;
        if (!APPLICATION_LIMITED_RECOVERY_ENABLED || !applicationLimited) {
            if (!applicationLimited) {
                applicationLimitedRecoveryPeriod = false;
                additionallyRecoveredThisPeriod.clear();
            }
            publishRecoveryQueueState(now);
            return;
        }
        if (!applicationLimitedRecoveryPeriod) {
            applicationLimitedRecoveryPeriod = true;
            additionallyRecoveredThisPeriod.clear();
        }
        publishRecoveryQueueState(now);
        if (!adaptive.allowsApplicationLimitedRecovery()) return;

        final long rtt = Math.max(PTO_TIMER_GRANULARITY_NANOS, config.getRTTNanos());
        if (lastPtoProbeSentNanos != 0L && now - lastPtoProbeSentNanos < rtt) return;
        final FrameSet candidate = selectAdditionalRecoveryCandidate(now, rtt);
        if (candidate == null) return;
        final int bytes = candidate.getRoughSize();
        if (adaptive.sendBudget(now, totalInFlightBytes(), bytes, queuedBytes) <= 0) return;

        candidate.touch("Application-limited recovery");
        ctx.write(candidate.retain()).addListener(RakNet.INTERNAL_WRITE_LISTENER);
        adaptive.onPacingBatchSent(1);
        additionalRecoveryBytesByFrameSet.addTo(candidate.getSeqId(), bytes);
        additionalRecoveryBytesInFlight += bytes;
        candidate.forEachRetriedReliableIndex(index -> {
            additionallyRecoveredThisPeriod.add(index);
            additionalRecoveryLastSent.put(index, now);
        });
        config.getMetrics().applicationLimitedRecovery(bytes);
        config.getMetrics().packetsOut(1);
        config.getMetrics().framesOut(candidate.getNumPackets());
        lastPtoProbeSentNanos = now; // share the speculative-send cooldown with PTO
        ctx.flush();
        refreshPtoTimer(now);
        publishRecoveryQueueState(now);
    }

    protected FrameSet selectAdditionalRecoveryCandidate(long now, long cooldownNanos) {
        final FrameSet directed = selectRemoteOrderedHolCandidate(
                remoteOrderedHolFeedback(now), true, now, cooldownNanos);
        if (directed != null) return directed;
        FrameSet oldest = null;
        FrameSet oldestOrdered = null;
        FrameSet highestDebtOrdered = null;
        for (FrameSet frameSet : pendingFrameSets.values()) {
            if (!isAdditionalRecoveryEligible(frameSet, now, cooldownNanos)) continue;
            if (oldest == null || frameSet.getSentTime() < oldest.getSentTime()) oldest = frameSet;
            if (frameSet.hasReliableOrderedFrame()
                    && (oldestOrdered == null || frameSet.getSentTime() < oldestOrdered.getSentTime())) {
                oldestOrdered = frameSet;
            }
            if (frameSet.hasRetriedReliableOrderedFrame()
                    && (highestDebtOrdered == null
                    || frameSet.maximumRetryCount() > highestDebtOrdered.maximumRetryCount()
                    || (frameSet.maximumRetryCount() == highestDebtOrdered.maximumRetryCount()
                    && frameSet.getSentTime() < highestDebtOrdered.getSentTime()))) {
                highestDebtOrdered = frameSet;
            }
        }
        if (highestDebtOrdered != null) return highestDebtOrdered;
        return oldestOrdered != null ? oldestOrdered : oldest;
    }

    private boolean isAdditionalRecoveryEligible(FrameSet frameSet, long now, long cooldownNanos) {
        if (!frameSet.hasRetriedReliableFrame()) return false;
        final boolean[] eligible = {true};
        frameSet.forEachRetriedReliableIndex(index -> {
            final long lastSent = additionalRecoveryLastSent.get(index);
            if (additionallyRecoveredThisPeriod.contains(index)
                    || (lastSent != Long.MIN_VALUE && now - lastSent < cooldownNanos)) {
                eligible[0] = false;
            }
        });
        return eligible[0];
    }

    private void publishRecoveryQueueState(long now) {
        int depth = 0;
        long oldestSent = now;
        for (FrameSet frameSet : pendingFrameSets.values()) {
            if (frameSet.hasRetriedReliableFrame()) {
                depth++;
                oldestSent = Math.min(oldestSent, frameSet.getSentTime());
            }
        }
        config.getMetrics().recoveryQueueState(depth,
                depth == 0 ? 0L : Math.max(0L, now - oldestSent));
        if (depth == 0) config.getMetrics().recoveryDebt(0D, -1);
    }

    int targetedFecChannel(int sequenceId) {
        return targetedFecChannelsByFrameSet.get(sequenceId);
    }

    double recoveryDebtForSequence(int sequenceId, long now) {
        final FrameSet frameSet = pendingFrameSets.get(sequenceId);
        final int channel = targetedFecChannelsByFrameSet.get(sequenceId);
        if (frameSet == null || channel < 0) return 0D;
        final long rtt = Math.max(PTO_TIMER_GRANULARITY_NANOS, config.getRTTNanos());
        final double ageInRtts = Math.max(0L, now - frameSet.getSentTime()) / (double) rtt;
        final double debt = Math.min(32D, frameSet.maximumRetryCount() + ageInRtts);
        config.getMetrics().recoveryDebt(debt, channel);
        return debt;
    }

    boolean tryAcquireTargetedFecBudget(int sequenceId, int bytes, long now) {
        if (!TARGETED_FEC_ENABLED || bytes <= 0
                || recoveryDebtForSequence(sequenceId, now) < TARGETED_FEC_MIN_DEBT
                || !adaptive.allowsApplicationLimitedRecovery()
                || adaptive.congestionWindowBlocked(totalInFlightBytes(), bytes)) return false;
        final long rtt = Math.max(PTO_TIMER_GRANULARITY_NANOS, config.getRTTNanos());
        if (targetedFecWindowStartedNanos == 0L || now - targetedFecWindowStartedNanos >= rtt) {
            targetedFecWindowStartedNanos = now;
            targetedFecBytesInWindow = 0;
        }
        final int budget = Math.max(256, config.getMTU());
        if (bytes > budget - targetedFecBytesInWindow) return false;
        targetedFecBytesInWindow += bytes;
        return true;
    }

    private void refreshRackLossTimer(long now) {
        if (!RACK_LOSS_DETECTION_ENABLED || latestAckedSentTimeNanos == 0L) {
            cancelRackLossTask();
            return;
        }
        final long reorderingWindow = rackReorderingWindowNanos(
                config.getRTTNanos(), rackReorderingMultiplierEighths);
        long nextDeadline = Long.MAX_VALUE;
        for (FrameSet frameSet : pendingFrameSets.values()) {
            if (frameSet.getSentTime() < latestAckedSentTimeNanos) {
                nextDeadline = Math.min(nextDeadline,
                        add(frameSet.getSentTime(), reorderingWindow));
            }
        }
        if (nextDeadline == Long.MAX_VALUE) cancelRackLossTask();
        else if (nextDeadline <= now) detectRackLosses(now);
        else scheduleRackLossCheck(nextDeadline);
    }

    private void scheduleRackLossCheck(long deadlineNanos) {
        if (ctx == null || !ctx.channel().isOpen()) return;
        if (rackLossFuture != null && !rackLossFuture.isDone()
                && rackLossDeadlineNanos <= deadlineNanos) return;
        cancelRackLossTask();
        rackLossDeadlineNanos = deadlineNanos;
        final long delay = Math.max(0L, deadlineNanos - System.nanoTime());
        rackLossFuture = ctx.executor().schedule(() -> {
            rackLossFuture = null;
            rackLossDeadlineNanos = Long.MAX_VALUE;
            if (ctx.channel().isOpen()) detectRackLosses(System.nanoTime());
        }, delay, TimeUnit.NANOSECONDS);
    }

    private void cancelRackLossTask() {
        if (rackLossFuture != null) rackLossFuture.cancel(false);
        rackLossFuture = null;
        rackLossDeadlineNanos = Long.MAX_VALUE;
    }

    private void pruneRackRetired(long now) {
        final ObjectIterator<Int2LongMap.Entry> iterator = rackRetiredFrameSets.int2LongEntrySet().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getLongValue() <= now) iterator.remove();
        }
    }

    private void onRackSpuriousAck() {
        rackCleanAckCount = 0;
        rackReorderingMultiplierEighths = Math.min(RACK_MAX_REORDERING_MULTIPLIER_EIGHTHS,
                rackReorderingMultiplierEighths + 2);
    }

    private void onRackCleanAck() {
        if (rackReorderingMultiplierEighths <= RACK_BASE_REORDERING_MULTIPLIER_EIGHTHS) return;
        if (++rackCleanAckCount >= RACK_CLEAN_ACKS_TO_DECAY) {
            rackCleanAckCount = 0;
            rackReorderingMultiplierEighths--;
        }
    }

    static long retransmissionTimeoutNanos(long rttNanos, long rttStdDevNanos,
                                           long retryDelayNanos, int retryCount) {
        final long rtt = Math.max(1, rttNanos);
        final long deviation = Math.max(0, rttStdDevNanos);
        final long delay = Math.max(0, retryDelayNanos);
        // NACKs provide fast recovery for observed gaps, so the timeout is a
        // conservative fallback. A two-RTT floor prevents delayed ACKs and
        // ordinary Internet jitter from creating duplicate reliable frames.
        final long jitterTimeout = add(add(rtt, multiply(deviation, 4)), delay);
        final long roundTripFloor = add(multiply(rtt, 2), delay);
        final long base = Math.max(jitterTimeout, roundTripFloor);
        return multiply(base, 1L << Math.min(Math.max(0, retryCount), 3));
    }

    static boolean shouldDeferNackGap(int missingCount) {
        return missingCount > 0 && missingCount <= MAX_DEFERRED_NACK_GAP;
    }

    static long nackReorderDelayNanos(long rttNanos, long rttStdDevNanos) {
        final long rttDelay = Math.max(0L, rttNanos) / 8L;
        final long jitterDelay = Math.max(0L, rttStdDevNanos) / 2L;
        final long candidate = Math.max(rttDelay, jitterDelay);
        return Math.max(MIN_NACK_REORDER_DELAY_NANOS,
                Math.min(MAX_NACK_REORDER_DELAY_NANOS, candidate));
    }

    static long nackRepeatDelayNanos(long rttNanos) {
        final long candidate = Math.max(0L, rttNanos) / 8L;
        return Math.max(MIN_NACK_REPEAT_DELAY_NANOS,
                Math.min(MAX_NACK_REPEAT_DELAY_NANOS, candidate));
    }

    private void deferNack(ChannelHandlerContext ctx, int sequenceId) {
        final long now = System.nanoTime();
        final long delay = nackReorderDelayNanos(config.getRTTNanos(), config.getRTTStdDevNanos());
        if (deferredNacks.defer(sequenceId, now + delay)) {
            config.getMetrics().nackDeferred(1);
        }
        scheduleDeferredNacks(ctx, delay);
    }

    private void scheduleDeferredNacks(ChannelHandlerContext ctx, long delayNanos) {
        final long delay = Math.max(0L, delayNanos);
        final long deadline = System.nanoTime() + delay;
        if (deferredNackFuture != null && !deferredNackFuture.isDone()) {
            if (deadline >= deferredNackDeadlineNanos) return;
            deferredNackFuture.cancel(false);
        }
        deferredNackDeadlineNanos = deadline;
        deferredNackFuture = ctx.executor().schedule(() -> flushDeferredNacks(ctx),
                delay, TimeUnit.NANOSECONDS);
    }

    private void flushDeferredNacks(ChannelHandlerContext ctx) {
        deferredNackFuture = null;
        deferredNackDeadlineNanos = Long.MAX_VALUE;
        if (!ctx.channel().isOpen()) return;
        if (!config.isNACKEnabled()) {
            deferredNacks.clear();
            return;
        }
        final long now = System.nanoTime();
        final long nextDelay = deferredNacks.drainDue(now, id -> {
            nackSet.add(id);
            config.getMetrics().nackDeferredExpired(1);
            adaptiveNackGrace.onLost(now, config.getRTTNanos());
        });
        publishRecoveryPolicies(now);
        if (!nackSet.isEmpty() && config.isAutoRead()) {
            sendResponses(ctx);
            ctx.flush();
        }
        if (nextDelay >= 0L) scheduleDeferredNacks(ctx, nextDelay);
    }

    private void confirmDeferredNacks() {
        cancelDeferredNackTask();
        final long now = System.nanoTime();
        deferredNacks.drainAll(id -> {
            nackSet.add(id);
            config.getMetrics().nackDeferredConfirmed(1);
            adaptiveNackGrace.onLost(now, config.getRTTNanos());
        });
        publishRecoveryPolicies(now);
    }

    private void cancelDeferredNackTask() {
        if (deferredNackFuture != null) deferredNackFuture.cancel(false);
        deferredNackFuture = null;
        deferredNackDeadlineNanos = Long.MAX_VALUE;
    }

    private boolean isNackRepeatActive(long nowNanos) {
        return ADAPTIVE_NACK_PROTECTION_ENABLED
                && (adaptiveNackGrace.isBypassing(nowNanos)
                || adaptiveAckProtection.isActive(nowNanos));
    }

    private void scheduleNackRepeat(ChannelHandlerContext ctx, long delayNanos) {
        if (nackRepeatFuture != null && !nackRepeatFuture.isDone()) return;
        nackRepeatFuture = ctx.executor().schedule(() -> flushRepeatedNacks(ctx),
                delayNanos, TimeUnit.NANOSECONDS);
    }

    private void flushRepeatedNacks(ChannelHandlerContext ctx) {
        nackRepeatFuture = null;
        final long now = System.nanoTime();
        publishRecoveryPolicies(now);
        if (!ctx.channel().isOpen() || nackRepeatSet.isEmpty()) {
            nackRepeatSet.clear();
            return;
        }
        if (!isNackRepeatActive(now) || !config.isNACKEnabled() || !config.isAutoRead()) {
            nackRepeatSet.clear();
            return;
        }
        final int requested = nackRepeatSet.size();
        ctx.write(new Reliability.NACK(nackRepeatSet)).addListener(RakNet.INTERNAL_WRITE_LISTENER);
        config.getMetrics().nackRepeated(requested);
        nackRepeatSet.clear();
        ctx.flush();
    }

    private void cancelNackRepeatTask() {
        if (nackRepeatFuture != null) nackRepeatFuture.cancel(false);
        nackRepeatFuture = null;
    }

    private void scheduleAckRepeat(ChannelHandlerContext ctx, long delayNanos) {
        if (ackRepeatFuture != null && !ackRepeatFuture.isDone()) return;
        ackRepeatFuture = ctx.executor().schedule(() -> flushRepeatedAcks(ctx),
                delayNanos, TimeUnit.NANOSECONDS);
    }

    private void flushRepeatedAcks(ChannelHandlerContext ctx) {
        ackRepeatFuture = null;
        final long now = System.nanoTime();
        publishRecoveryPolicies(now);
        if (!ctx.channel().isOpen() || ackRepeatSet.isEmpty()) {
            ackRepeatSet.clear();
            return;
        }
        if (!adaptiveAckProtection.isActive(now)) {
            ackRepeatSet.clear();
            return;
        }
        final int acknowledged = ackRepeatSet.size();
        ctx.write(new Reliability.ACK(ackRepeatSet)).addListener(RakNet.INTERNAL_WRITE_LISTENER);
        config.getMetrics().ackRepeated(acknowledged);
        ackRepeatSet.clear();
        ctx.flush();
    }

    private void cancelAckRepeatTask() {
        if (ackRepeatFuture != null) ackRepeatFuture.cancel(false);
        ackRepeatFuture = null;
    }

    private void publishRecoveryPolicies(long now) {
        final boolean nackBypass = ADAPTIVE_NACK_GRACE_ENABLED && adaptiveNackGrace.isBypassing(now);
        config.getMetrics().adaptiveNackGrace(nackBypass);
        final boolean ackProtection = ADAPTIVE_ACK_PROTECTION_ENABLED && adaptiveAckProtection.isActive(now);
        config.getMetrics().adaptiveAckPolicy(ackProtection, ACK_FLUSH_DELAY_NANOS,
                ackProtection ? adaptiveAckProtection.repeatDelayNanos(config.getRTTNanos()) : 0L);
    }

    protected void produceFrameSet(ChannelHandlerContext ctx, int maxSize) {
        if (frameQueue.isEmpty()) return;
        final FrameSet frameSet = FrameSet.create();
        int minRetryCount = Integer.MAX_VALUE;
        Frame frame;
        while ((frame = frameQueue.peek()) != null) {
            assert frame.refCnt() > 0 : "Frame has lost reference";
            final int roughPacketSize = frame.getRoughPacketSize();
            if (frameSet.getRoughSize() + roughPacketSize > maxSize) {
                if (frameSet.isEmpty()) {
                    throw new CorruptedFrameException(
                            "Finished frame larger than the MTU by " + (roughPacketSize - maxSize));
                }
                break;
            }
            frameQueue.poll();
            this.queuedBytes -= roughPacketSize;
            minRetryCount = Math.min(minRetryCount, frame.getRetryCount());
            frameSet.addPacket(frame);
        }
        if (!frameSet.isEmpty()) {
            frameSet.setRetryCount(minRetryCount == Integer.MAX_VALUE ? 0 : minRetryCount);
            frameSet.setSeqId(nextSendSeqId);
            nextSendSeqId = UINT.B3.plus(nextSendSeqId, 1);
            pendingFrameSets.put(frameSet.getSeqId(), frameSet);
            final int targetedFecChannel = frameSet.retriedOrderedChannel();
            if (targetedFecChannel >= 0) {
                targetedFecChannelsByFrameSet.put(frameSet.getSeqId(), targetedFecChannel);
            }
            inFlightBytes += frameSet.getRoughSize();
            frameSet.touch("Added to pending FrameSet list");
            ctx.write(frameSet.retain()).addListener(RakNet.INTERNAL_WRITE_LISTENER);
            config.getMetrics().packetsOut(1);
            config.getMetrics().framesOut(frameSet.getNumPackets());
            config.getMetrics().currentQueuedBytes(this.queuedBytes);
            if (frameSet.hasReliableFrame()) refreshPtoTimer(System.nanoTime());
            assert frameSet.refCnt() > 0;
        } else {
            frameSet.release();
        }
    }

    protected void produceFrameSets(ChannelHandlerContext ctx) {
        if (frameQueue.isEmpty()) {
            final long now = System.nanoTime();
            adaptive.onOutstandingBytes(now, totalInFlightBytes());
            if (pendingFrameSets.isEmpty()) adaptive.applyPendingMtu();
            updateApplicationLimitedRecovery(now);
            return;
        }
        applicationLimitedRecoveryPeriod = false;
        additionallyRecoveredThisPeriod.clear();
        final int mtu = config.getMTU();
        final int maxSize = mtu - FrameSet.HEADER_SIZE - Frame.HEADER_SIZE;
        final int maxPendingFrameSets = config.getDefaultPendingFrameSets() + burstTokens;
        int pacingBudget = adaptive.sendBudget(System.nanoTime(), totalInFlightBytes(), mtu, queuedBytes);
        int pacedDatagrams = 0;
        while (pacingBudget-- > 0 && pendingFrameSets.size() < maxPendingFrameSets && !frameQueue.isEmpty()) {
            produceFrameSet(ctx, maxSize);
            pacedDatagrams++;
        }
        adaptive.onPacingBatchSent(pacedDatagrams);
        adaptive.applyDscp(ctx.channel().parent());
        if (config.isAdaptiveTransportEnabled() && !frameQueue.isEmpty()
                && pendingFrameSets.size() < maxPendingFrameSets
                && !adaptive.congestionWindowBlocked(totalInFlightBytes(), mtu)
                && !pacingScheduled) {
            pacingScheduled = true;
            pacingFuture = ctx.executor().schedule(() -> {
                pacingFuture = null;
                pacingScheduled = false;
                if (!removed && ctx.channel().isOpen()) flush(ctx);
            }, adaptive.nanosUntilSend(System.nanoTime()), TimeUnit.NANOSECONDS);
        }
        if (frameQueue.isEmpty()) updateApplicationLimitedRecovery(System.nanoTime());
    }

    protected void tryProduceFrameSets() {
        if (removed || productionScheduled || ctx == null) return;
        productionScheduled = true;
        ctx.executor().execute(() -> {
            productionScheduled = false;
            if (removed || !ctx.channel().isOpen()) return;
            produceFrameSets(ctx);
            ctx.flush();
        });
    }

    private void cancelTransportTasks() {
        cancelAckFlushTask();
        if (coalesceFuture != null) coalesceFuture.cancel(false);
        coalesceFuture = null;
        coalesceScheduled = false;
        if (pacingFuture != null) pacingFuture.cancel(false);
        pacingFuture = null;
        pacingScheduled = false;
        productionScheduled = false;
    }

    private void cancelAckFlushTask() {
        if (ackFlushFuture != null) ackFlushFuture.cancel(false);
        ackFlushFuture = null;
    }

    //TODO: instead of immediate recall, mark framesets as 'recalled', and flush at flush cycle
    protected void recallFrameSet(FrameSet frameSet) {
        try {
            adjustResendGauge(-1);
            config.getMetrics().bytesRecalled(frameSet.getRoughSize());
            frameSet.touch("Recalled");
            frameSet.createFrames(frame -> {
                if (frame.getReliability().isReliable) {
                    frame.incRetryCount();
                    queueFrame(frame);
                } else {
                    frame.getPromise().trySuccess(); //TODO: maybe need a fail here
                    frame.release();
                }
            });
            tryProduceFrameSets();
            flush(ctx);
        } finally {
            frameSet.release();
        }
    }

    protected void updateBackPressure(ChannelHandlerContext ctx) {
        final int queuedBytes = getQueuedBytes();
        final boolean oldWritable = ctx.channel().attr(RakNet.WRITABLE).get();
        boolean newWritable = oldWritable;
        if (queuedBytes > config.getMaxQueuedBytes()) {
            final CodecException t = new CodecException("Frame queue is too large");
            clearQueue(t);
            ctx.close();
            throw t;
        } else if (queuedBytes > config.getWriteBufferHighWaterMark()) {
            newWritable = false;
        } else if (queuedBytes < config.getWriteBufferLowWaterMark()) {
            newWritable = true;
        }
        if (newWritable != oldWritable) {
            ctx.channel().attr(RakNet.WRITABLE).set(newWritable ? Boolean.TRUE : Boolean.FALSE);
            ctx.fireChannelWritabilityChanged();
        }
    }

    protected int getQueuedBytes() {
//        int byteSize = 0;
//        for (Frame frame : frameQueue) {
//            byteSize += frame.getRoughPacketSize();
//        }
//        return byteSize;
        return this.queuedBytes;
    }

    AdaptiveTransportController adaptiveController() { return adaptive; }

}
