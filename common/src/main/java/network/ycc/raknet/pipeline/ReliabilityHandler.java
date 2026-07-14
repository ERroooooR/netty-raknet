package network.ycc.raknet.pipeline;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
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

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

/**
 * This handler handles the bulk of reliable (framed) transport.
 */
public class ReliabilityHandler extends ChannelDuplexHandler {

    public static final String NAME = "rn-reliability";

    protected final IntSortedSet nackSet = new IntRBTreeSet(UINT.B3.COMPARATOR);
    protected final IntSortedSet ackSet = new IntRBTreeSet(UINT.B3.COMPARATOR);
    protected final PriorityQueue<Frame> frameQueue = new PriorityQueue<>(Frame.COMPARATOR);
    protected final Int2ObjectLinkedOpenHashMap<FrameSet> pendingFrameSets = new Int2ObjectLinkedOpenHashMap<>();
    protected final ReliableReceiveWindow reliableReceiveWindow = new ReliableReceiveWindow();
    protected final DeferredNackTracker deferredNacks = new DeferredNackTracker();

    protected int queuedBytes = 0;
    protected long inFlightBytes = 0;

    protected int lastReceivedSeqId = 0;
    protected int nextSendSeqId = 0;
    protected int resendGauge = 0;
    protected int burstTokens = 0;
    protected RakNet.Config config = null; //TODO: not really needed anymore
    protected ChannelHandlerContext ctx;
    protected AdaptiveTransportController adaptive;
    protected boolean pacingScheduled;
    protected boolean coalesceScheduled;
    protected int coalescedFrames;
    protected long coalesceStartedNanos;
    protected ScheduledFuture<?> deferredNackFuture;
    protected ScheduledFuture<?> ackFlushFuture;
    protected ScheduledFuture<?> ackRepeatFuture;
    protected long lastAckRepeatNanos;

    // Item 3: track when first ACK was queued for time-based flush
    protected long firstAckNanos = 0;
    private static final long MIN_NACK_REORDER_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(3);
    private static final long MAX_NACK_REORDER_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(12);
    private static final long ACK_REPEAT_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    // Item 7: guard against recursive flush from recallFrameSet
    protected boolean flushing = false;

    protected Runnable frameSetProduction = () -> {
        this.produceFrameSets(ctx);
        ctx.flush();
    };

    public void onNegotiatedMtu(int mtu) {
        if (adaptive != null) adaptive.onNegotiatedMtu(mtu);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        config = RakNet.config(ctx);
        this.ctx = ctx;
        this.adaptive = new AdaptiveTransportController(config);
        ctx.channel().attr(RakNet.WRITABLE).set(true);
        if (config.isIgnoreResendGauge()) this.resendGauge = 2;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (deferredNackFuture != null) deferredNackFuture.cancel(false);
        deferredNackFuture = null;
        if (ackFlushFuture != null) ackFlushFuture.cancel(false);
        ackFlushFuture = null;
        if (ackRepeatFuture != null) ackRepeatFuture.cancel(false);
        ackRepeatFuture = null;
        deferredNacks.clear();
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
                ctx.executor().schedule(() -> {
                    coalesceScheduled = false;
                    if (ctx.channel().isOpen() && !frameQueue.isEmpty()) {
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
        pendingFrameSets.values().forEach(FrameSet::release);
        pendingFrameSets.clear();
    }

    protected void readFrameSet(ChannelHandlerContext ctx, FrameSet frameSet) {
        final int packetSeqId = frameSet.getSeqId();
        // Item 3: track first ACK time for time-based flush trigger
        if (ackSet.isEmpty()) {
            firstAckNanos = System.nanoTime();
            // Schedule guaranteed ACK flush so low-traffic connections
            // don't wait until the 50ms flush tick or next FrameSet arrival.
            scheduleAckFlush(ctx, adaptive.ackFlushDelayNanos());
        }
        ackSet.add(packetSeqId);
        if (config.isNACKEnabled()) {
            nackSet.remove(packetSeqId);
            if (deferredNacks.cancel(packetSeqId)) {
                config.getMetrics().reorderedPacket(1);
            }
        }
        if (UINT.B3.minusWrap(packetSeqId, lastReceivedSeqId) > 0) {
            lastReceivedSeqId = UINT.B3.plus(lastReceivedSeqId, 1);
            while (lastReceivedSeqId != packetSeqId) { //nack any missed packets before this one
                if (config.isNACKEnabled()) {
                    deferNack(ctx, lastReceivedSeqId);
                }
                lastReceivedSeqId = UINT.B3.plus(lastReceivedSeqId, 1);
            }
        }
        config.getMetrics().packetsIn(1);
        config.getMetrics().framesIn(frameSet.getNumPackets());
        frameSet.createFrames(frame -> {
            if (frame.getReliability().isReliable
                    && !reliableReceiveWindow.accept(frame.getReliableIndex())) {
                // Retransmitted FrameSets use a fresh sequence ID, so ACKing the
                // FrameSet is not enough to suppress duplicate reliable frames.
                // In particular, a duplicate split fragment can otherwise create
                // an orphan FrameJoiner builder after the original was completed.
                config.getMetrics().reliableFrameDuplicate(1);
                frame.release();
                return;
            }
            ctx.fireChannelRead(frame);
        });
        trySendResponses(ctx);
        ctx.fireChannelReadComplete();
    }

    protected void readAck(Reliability.ACK ack) {
        int ackdBytes = 0;
        int nIterations = 0;
        for (Reliability.REntry entry : ack.getEntries()) {
            final int max = UINT.B3.plus(entry.idFinish, 1);
            for (int id = entry.idStart; id != max; id = UINT.B3.plus(id, 1)) {
                final FrameSet frameSet = pendingFrameSets.remove(id);
                if (frameSet != null) {
                    ackdBytes += frameSet.getRoughSize();
                    inFlightBytes = Math.max(0, inFlightBytes - frameSet.getRoughSize());
                    adaptive.onAck(frameSet.getRoughSize(), System.nanoTime() - frameSet.getSentTime(),
                            inFlightBytes, frameSet.isApplicationLimited());
                    adjustResendGauge(1);
                    frameSet.succeed();
                    frameSet.release();
                    tryProduceFrameSets();
                }
                Constants.packetLossCheck(nIterations++, "ack confirm range");
            }
        }
        config.getMetrics().bytesACKd(ackdBytes);
    }

    protected void readNack(Reliability.NACK nack) {
        if (!config.isNACKEnabled()) return;

        int bytesNACKd = 0;
        int nIterations = 0;
        for (Reliability.REntry entry : nack.getEntries()) {
            final int max = UINT.B3.plus(entry.idFinish, 1);
            for (int id = entry.idStart; id != max; id = UINT.B3.plus(id, 1)) {
                final FrameSet frameSet = pendingFrameSets.remove(id);
                if (frameSet != null) {
                    bytesNACKd += frameSet.getRoughSize();
                    inFlightBytes = Math.max(0, inFlightBytes - frameSet.getRoughSize());
                    adaptive.onLoss(frameSet.getRoughSize(), false);
                    recallFrameSet(frameSet);
                }
                Constants.packetLossCheck(nIterations++, "nack confirm range");
            }
        }
        config.getMetrics().bytesNACKd(bytesNACKd);
        config.getMetrics().nackRetransmit(bytesNACKd);
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
        final long ackDelayNanos = adaptive.ackFlushDelayNanos();
        final boolean ackReady = !ackSet.isEmpty()
                && (ackSet.size() >= config.getDefaultPendingFrameSets() - 1
                || System.nanoTime() - firstAckNanos >= ackDelayNanos);
        if (ackReady) {
            final int acknowledgedFrameSets = ackSet.size();
            final Reliability.ACK ack = new Reliability.ACK(ackSet);
            final long now = System.nanoTime();
            final boolean repeat = adaptive.shouldProtectAcks()
                    && now - lastAckRepeatNanos >= ACK_REPEAT_INTERVAL_NANOS;
            final Reliability.ACK repeatedAck = repeat ? new Reliability.ACK(ackSet) : null;
            ctx.write(ack).addListener(RakNet.INTERNAL_WRITE_LISTENER);
            config.getMetrics().acksSent(acknowledgedFrameSets);
            ackSet.clear();
            firstAckNanos = 0;
            cancelAckFlush();
            if (repeatedAck != null) {
                lastAckRepeatNanos = now;
                ackRepeatFuture = ctx.executor().schedule(() -> {
                    ackRepeatFuture = null;
                    if (!ctx.channel().isOpen()) return;
                    ctx.write(repeatedAck).addListener(RakNet.INTERNAL_WRITE_LISTENER);
                    ctx.flush();
                    config.getMetrics().ackRepeated(acknowledgedFrameSets);
                }, adaptive.ackRepeatDelayNanos(), TimeUnit.NANOSECONDS);
            }
        } else if (!ackSet.isEmpty()) {
            scheduleAckFlush(ctx, Math.max(0L,
                    ackDelayNanos - (System.nanoTime() - firstAckNanos)));
        }
        if (config.isNACKEnabled() && !nackSet.isEmpty() && config.isAutoRead()) { //only nack if we can read
            ctx.write(new Reliability.NACK(nackSet)).addListener(RakNet.INTERNAL_WRITE_LISTENER);
            config.getMetrics().nacksSent(nackSet.size());
            nackSet.clear();
        }
    }

    protected void trySendResponses(ChannelHandlerContext ctx) {
        // Send ACKs on count or an adaptive time threshold, whichever comes first.
        // A policer episode uses a longer aggregation window so duplicate data
        // does not turn into an ACK packet storm.
        final boolean countTrigger = ackSet.size() >= config.getDefaultPendingFrameSets() - 1;
        final boolean timeTrigger = !ackSet.isEmpty()
                && System.nanoTime() - firstAckNanos >= adaptive.ackFlushDelayNanos();
        if (countTrigger || timeTrigger) {
            sendResponses(ctx);
            ctx.flush();
        }
    }

    private void scheduleAckFlush(ChannelHandlerContext ctx, long delayNanos) {
        if (ackFlushFuture != null) return;
        ackFlushFuture = ctx.executor().schedule(() -> {
            ackFlushFuture = null;
            if (!ctx.channel().isOpen() || ackSet.isEmpty()) return;
            final long remaining = adaptive.ackFlushDelayNanos()
                    - (System.nanoTime() - firstAckNanos);
            if (remaining > 0) {
                scheduleAckFlush(ctx, remaining);
            } else {
                sendResponses(ctx);
                ctx.flush();
            }
        }, Math.max(0L, delayNanos), TimeUnit.NANOSECONDS);
    }

    private void cancelAckFlush() {
        if (ackFlushFuture != null) {
            ackFlushFuture.cancel(false);
            ackFlushFuture = null;
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
    }

    private static long saturatedAdd(long a, long b) {
        return a > Long.MAX_VALUE - b ? Long.MAX_VALUE : a + b;
    }

    private static long saturatedMultiply(long value, long multiplier) {
        return value > Long.MAX_VALUE / multiplier ? Long.MAX_VALUE : value * multiplier;
    }

    static long retransmissionTimeoutNanos(long rttNanos, long rttStdDevNanos,
                                           long retryDelayNanos, int retryCount) {
        final long rtt = Math.max(1, rttNanos);
        final long deviation = Math.max(0, rttStdDevNanos);
        final long delay = Math.max(0, retryDelayNanos);
        // NACKs provide fast recovery for observed gaps, so the timeout is a
        // conservative fallback. A two-RTT floor prevents delayed ACKs and
        // ordinary Internet jitter from creating duplicate reliable frames.
        final long jitterTimeout = saturatedAdd(saturatedAdd(rtt,
                saturatedMultiply(deviation, 4)), delay);
        final long roundTripFloor = saturatedAdd(saturatedMultiply(rtt, 2), delay);
        final long base = Math.max(jitterTimeout, roundTripFloor);
        return saturatedMultiply(base, 1L << Math.min(Math.max(0, retryCount), 3));
    }

    static long nackReorderDelayNanos(long rttNanos) {
        final long candidate = Math.max(0L, rttNanos) / 8L;
        return Math.max(MIN_NACK_REORDER_DELAY_NANOS,
                Math.min(MAX_NACK_REORDER_DELAY_NANOS, candidate));
    }

    private void deferNack(ChannelHandlerContext ctx, int sequenceId) {
        final long now = System.nanoTime();
        final long delay = adaptive.adjustNackReorderDelayNanos(
                nackReorderDelayNanos(config.getRTTNanos()));
        if (deferredNacks.defer(sequenceId, now + delay)) {
            config.getMetrics().nackDeferred(1);
        }
        scheduleDeferredNacks(ctx, delay);
    }

    private void scheduleDeferredNacks(ChannelHandlerContext ctx, long delayNanos) {
        if (deferredNackFuture != null) return;
        deferredNackFuture = ctx.executor().schedule(() -> flushDeferredNacks(ctx),
                Math.max(0L, delayNanos), TimeUnit.NANOSECONDS);
    }

    private void flushDeferredNacks(ChannelHandlerContext ctx) {
        deferredNackFuture = null;
        if (!ctx.channel().isOpen()) return;
        final long nextDelay = deferredNacks.drainDue(System.nanoTime(), id -> nackSet.add(id));
        if (!nackSet.isEmpty() && config.isAutoRead()) {
            sendResponses(ctx);
            ctx.flush();
        }
        if (nextDelay >= 0L) scheduleDeferredNacks(ctx, nextDelay);
    }

    static final class DeferredNackTracker {
        private final Int2LongOpenHashMap deadlines = new Int2LongOpenHashMap();

        boolean defer(int sequenceId, long deadlineNanos) {
            if (deadlines.containsKey(sequenceId)) return false;
            deadlines.put(sequenceId, deadlineNanos);
            return true;
        }

        boolean cancel(int sequenceId) {
            return deadlines.remove(sequenceId) != 0L;
        }

        long drainDue(long nowNanos, IntConsumer consumer) {
            long nextDeadline = Long.MAX_VALUE;
            final ObjectIterator<Int2LongMap.Entry> iterator = deadlines.int2LongEntrySet().fastIterator();
            while (iterator.hasNext()) {
                final Int2LongMap.Entry entry = iterator.next();
                if (entry.getLongValue() <= nowNanos) {
                    consumer.accept(entry.getIntKey());
                    iterator.remove();
                } else {
                    nextDeadline = Math.min(nextDeadline, entry.getLongValue());
                }
            }
            return nextDeadline == Long.MAX_VALUE ? -1L : Math.max(0L, nextDeadline - nowNanos);
        }

        void clear() {
            deadlines.clear();
        }
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
            // A delivery sample produced after draining the application queue
            // measures demand, not path capacity. Carry that information with
            // the FrameSet until its ACK arrives.
            frameSet.setApplicationLimited(frameQueue.isEmpty());
            frameSet.setRetryCount(minRetryCount == Integer.MAX_VALUE ? 0 : minRetryCount);
            frameSet.setSeqId(nextSendSeqId);
            nextSendSeqId = UINT.B3.plus(nextSendSeqId, 1);
            pendingFrameSets.put(frameSet.getSeqId(), frameSet);
            inFlightBytes += frameSet.getRoughSize();
            adaptive.onDatagramSent(frameSet.getRoughSize());
            frameSet.touch("Added to pending FrameSet list");
            ctx.write(frameSet.retain()).addListener(RakNet.INTERNAL_WRITE_LISTENER);
            config.getMetrics().packetsOut(1);
            config.getMetrics().framesOut(frameSet.getNumPackets());
            config.getMetrics().currentQueuedBytes(this.queuedBytes);
            assert frameSet.refCnt() > 0;
        } else {
            frameSet.release();
        }
    }

    protected void produceFrameSets(ChannelHandlerContext ctx) {
        if (frameQueue.isEmpty()) {
            if (pendingFrameSets.isEmpty()) adaptive.applyPendingMtu();
            return;
        }
        final int mtu = config.getMTU();
        final int maxSize = mtu - FrameSet.HEADER_SIZE - Frame.HEADER_SIZE;
        final int maxPendingFrameSets = config.getDefaultPendingFrameSets() + burstTokens;
        final Frame nextFrame = frameQueue.peek();
        final int estimatedDatagramBytes = nextFrame == null ? mtu
                : Math.min(mtu, FrameSet.HEADER_SIZE + nextFrame.getRoughPacketSize());
        int pacingBudget = pendingFrameSets.size() < maxPendingFrameSets
                ? adaptive.sendBudget(System.nanoTime(), inFlightBytes, estimatedDatagramBytes) : 0;
        while (pacingBudget-- > 0 && pendingFrameSets.size() < maxPendingFrameSets && !frameQueue.isEmpty()) {
            produceFrameSet(ctx, maxSize);
        }
        adaptive.applyDscp(ctx.channel().parent());
        if (config.isAdaptiveTransportEnabled() && !frameQueue.isEmpty()
                && pendingFrameSets.size() < maxPendingFrameSets
                && !adaptive.congestionWindowBlocked(inFlightBytes, mtu)
                && !pacingScheduled) {
            pacingScheduled = true;
            ctx.executor().schedule(() -> {
                pacingScheduled = false;
                if (ctx.channel().isOpen()) flush(ctx);
            }, adaptive.nanosUntilSend(System.nanoTime()), TimeUnit.NANOSECONDS);
        }
    }

    protected void tryProduceFrameSets() {
        ctx.executor().execute(frameSetProduction);
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
