package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.packet.FrameSet;
import network.ycc.raknet.packet.Reliability;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReliabilityAdaptiveRecoveryHandlerTest {

    @Test
    public void learnedTrueLossBypassesGraceInFrameSetProcessing() {
        final Fixture fixture = new Fixture();
        fixture.enableNacks();
        final long now = System.nanoTime();
        for (int i = 0; i < 8; i++) {
            fixture.handler.adaptiveNackGrace.onLost(now + i, fixture.rttNanos);
        }
        Assertions.assertTrue(fixture.handler.adaptiveNackGrace.isBypassing(System.nanoTime()));
        Assertions.assertFalse(fixture.handler.adaptiveNackGrace.shouldDefer(System.nanoTime()));

        final FrameSet frameSet = FrameSet.create();
        try {
            frameSet.setSeqId(2);
            fixture.handler.readFrameSet(fixture.ctx, frameSet);
        } finally {
            frameSet.release();
        }

        // trySendResponses may already have flushed the NACK if mock processing
        // exceeded the 2 ms ACK deadline. Flush any remaining response and
        // inspect the actual outbound messages in either case.
        fixture.handler.sendResponses(fixture.ctx);
        final ArgumentCaptor<Object> writes = ArgumentCaptor.forClass(Object.class);
        verify(fixture.ctx, atLeast(1)).write(writes.capture());
        final boolean immediateNack = writes.getAllValues().stream()
                .filter(Reliability.NACK.class::isInstance)
                .map(Reliability.NACK.class::cast)
                .flatMap(nack -> java.util.Arrays.stream(nack.getEntries()))
                .anyMatch(entry -> entry.idStart <= 1 && entry.idFinish >= 1);
        Assertions.assertTrue(immediateNack);
        Assertions.assertFalse(fixture.handler.deferredNacks.cancel(1));
        verify(fixture.metrics).nackGraceBypassed(1);
        verify(fixture.metrics, never()).nackDeferred(1);
    }

    @Test
    public void protectedAckBatchesAreCoalescedIntoOneRepeat() {
        final Fixture fixture = new Fixture();
        fixture.activateAckProtection();
        final ArgumentCaptor<Runnable> repeatTask = ArgumentCaptor.forClass(Runnable.class);
        doReturn(fixture.scheduledFuture).when(fixture.executor)
                .schedule(repeatTask.capture(), anyLong(), eq(TimeUnit.NANOSECONDS));
        when(fixture.scheduledFuture.isDone()).thenReturn(false);

        fixture.handler.ackSet.add(10);
        fixture.handler.sendResponses(fixture.ctx);
        fixture.handler.ackSet.add(11);
        fixture.handler.sendResponses(fixture.ctx);

        verify(fixture.executor, times(1))
                .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS));
        repeatTask.getValue().run();

        final ArgumentCaptor<Object> writes = ArgumentCaptor.forClass(Object.class);
        verify(fixture.ctx, times(3)).write(writes.capture());
        Assertions.assertTrue(writes.getAllValues().get(0) instanceof Reliability.ACK);
        Assertions.assertTrue(writes.getAllValues().get(1) instanceof Reliability.ACK);
        final Reliability.ACK repeated = (Reliability.ACK) writes.getAllValues().get(2);
        Assertions.assertEquals(1, repeated.getEntries().length);
        Assertions.assertEquals(10, repeated.getEntries()[0].idStart);
        Assertions.assertEquals(11, repeated.getEntries()[0].idFinish);
        verify(fixture.metrics).ackRepeated(2);
        verify(fixture.ctx, atLeastOnce()).flush();
    }

    @Test
    public void protectedNackBatchesAreRepeatedOnlyOnce() {
        final Fixture fixture = new Fixture();
        fixture.enableNacks();
        fixture.activateAckProtection();
        final ArgumentCaptor<Runnable> repeatTask = ArgumentCaptor.forClass(Runnable.class);
        doReturn(fixture.scheduledFuture).when(fixture.executor)
                .schedule(repeatTask.capture(), anyLong(), eq(TimeUnit.NANOSECONDS));
        when(fixture.scheduledFuture.isDone()).thenReturn(false);

        fixture.handler.nackSet.add(20);
        fixture.handler.sendResponses(fixture.ctx);
        fixture.handler.nackSet.add(21);
        fixture.handler.sendResponses(fixture.ctx);

        verify(fixture.executor, times(1))
                .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS));
        repeatTask.getValue().run();

        final ArgumentCaptor<Object> writes = ArgumentCaptor.forClass(Object.class);
        verify(fixture.ctx, times(3)).write(writes.capture());
        Assertions.assertTrue(writes.getAllValues().get(0) instanceof Reliability.NACK);
        Assertions.assertTrue(writes.getAllValues().get(1) instanceof Reliability.NACK);
        final Reliability.NACK repeated = (Reliability.NACK) writes.getAllValues().get(2);
        Assertions.assertEquals(1, repeated.getEntries().length);
        Assertions.assertEquals(20, repeated.getEntries()[0].idStart);
        Assertions.assertEquals(21, repeated.getEntries()[0].idFinish);
        verify(fixture.metrics).nackRepeated(2);
        verify(fixture.ctx, atLeastOnce()).flush();
    }

    @Test
    public void arrivingFrameCancelsItsPendingNackRepeat() {
        final Fixture fixture = new Fixture();
        fixture.enableNacks();
        fixture.activateAckProtection();
        final ArgumentCaptor<Runnable> repeatTask = ArgumentCaptor.forClass(Runnable.class);
        doReturn(fixture.scheduledFuture).when(fixture.executor)
                .schedule(repeatTask.capture(), anyLong(), eq(TimeUnit.NANOSECONDS));
        when(fixture.scheduledFuture.isDone()).thenReturn(false);

        fixture.handler.nackSet.add(30);
        fixture.handler.sendResponses(fixture.ctx);
        fixture.handler.lastReceivedSeqId = 29;
        final FrameSet arrived = FrameSet.create();
        try {
            arrived.setSeqId(30);
            fixture.handler.readFrameSet(fixture.ctx, arrived);
        } finally {
            arrived.release();
        }
        repeatTask.getValue().run();

        verify(fixture.metrics, never()).nackRepeated(1);
        final ArgumentCaptor<Object> writes = ArgumentCaptor.forClass(Object.class);
        verify(fixture.ctx, atLeastOnce()).write(writes.capture());
        Assertions.assertEquals(1L, writes.getAllValues().stream()
                .filter(Reliability.NACK.class::isInstance).count());
    }

    @Test
    public void nackRepeatDelayIsBoundedByPathRtt() {
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(5),
                ReliabilityHandler.nackRepeatDelayNanos(TimeUnit.MILLISECONDS.toNanos(8)));
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(10),
                ReliabilityHandler.nackRepeatDelayNanos(TimeUnit.MILLISECONDS.toNanos(80)));
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(20),
                ReliabilityHandler.nackRepeatDelayNanos(TimeUnit.MILLISECONDS.toNanos(400)));
    }

    @Test
    public void rackReorderingWindowIsBoundedAndAdaptive() {
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(100),
                ReliabilityHandler.rackReorderingWindowNanos(
                        TimeUnit.MILLISECONDS.toNanos(80), 10));
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(160),
                ReliabilityHandler.rackReorderingWindowNanos(
                        TimeUnit.MILLISECONDS.toNanos(80), 16));
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(1),
                ReliabilityHandler.rackReorderingWindowNanos(1, 10));
    }

    @Test
    public void newerAckCausesOldOutstandingFrameSetToBeRecalledByRack() {
        final Fixture fixture = new Fixture();
        final FrameSet older = FrameSet.create();
        final FrameSet newer = FrameSet.create();
        older.setSeqId(40);
        newer.setSeqId(41);
        fixture.handler.pendingFrameSets.put(40, older);
        fixture.handler.pendingFrameSets.put(41, newer);
        fixture.handler.inFlightBytes = older.getRoughSize() + newer.getRoughSize();

        fixture.handler.readAck(new Reliability.ACK(41));
        Assertions.assertTrue(fixture.handler.pendingFrameSets.containsKey(40));

        final long deadline = older.getSentTime()
                + ReliabilityHandler.rackReorderingWindowNanos(fixture.rttNanos, 10);
        fixture.handler.detectRackLosses(deadline);

        Assertions.assertFalse(fixture.handler.pendingFrameSets.containsKey(40));
        Assertions.assertEquals(1, fixture.handler.rackRecalls);
        verify(fixture.metrics).rackRetransmit(older.getRoughSize());
        fixture.handler.readAck(new Reliability.ACK(40));
        verify(fixture.metrics).rackSpuriousAck(1);
        Assertions.assertEquals(12, fixture.handler.rackReorderingMultiplierEighths);
    }

    @Test
    public void ptoCalculationUsesVariationAndExponentialBackoff() {
        final long base = ReliabilityHandler.ptoTimeoutNanos(
                TimeUnit.MILLISECONDS.toNanos(80), TimeUnit.MILLISECONDS.toNanos(5),
                TimeUnit.MILLISECONDS.toNanos(2), 0);
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(102), base);
        Assertions.assertEquals(base * 4L, ReliabilityHandler.ptoTimeoutNanos(
                TimeUnit.MILLISECONDS.toNanos(80), TimeUnit.MILLISECONDS.toNanos(5),
                TimeUnit.MILLISECONDS.toNanos(2), 2));
    }

    @Test
    public void ptoPrefersReliableOrderedDataAndAccountsProbeUntilAck() {
        final Fixture fixture = new Fixture();
        final FrameSet unordered = reliableFrameSet(50, false, 0);
        final FrameSet ordered = reliableFrameSet(51, true, 2);
        fixture.handler.pendingFrameSets.put(50, unordered);
        fixture.handler.pendingFrameSets.put(51, ordered);
        fixture.handler.inFlightBytes = unordered.getRoughSize() + ordered.getRoughSize();

        Assertions.assertSame(ordered, fixture.handler.selectPtoProbeCandidate());
        final int probeBytes = ordered.getRoughSize();
        fixture.handler.sendPtoProbe(System.nanoTime());

        verify(fixture.ctx).write(ordered);
        verify(fixture.metrics).ptoProbe(probeBytes);
        Assertions.assertEquals(probeBytes, fixture.handler.ptoProbeBytesInFlight);
        Assertions.assertEquals(1, fixture.handler.ptoCount);

        fixture.handler.readAck(new Reliability.ACK(51));
        verify(fixture.metrics).ptoProbeAcked(probeBytes);
        Assertions.assertEquals(0L, fixture.handler.ptoProbeBytesInFlight);
        Assertions.assertEquals(0, fixture.handler.ptoCount);

        // The mocked outbound pipeline owns the retain performed by sendPtoProbe.
        ordered.release();
        fixture.handler.pendingFrameSets.remove(50).release();
    }

    @Test
    public void applicationLimitedRecoverySendsOncePerLogicalEntryAndPeriod() {
        final Fixture fixture = new Fixture();
        final FrameSet retried = reliableFrameSet(60, true, 3, true);
        fixture.handler.pendingFrameSets.put(60, retried);
        fixture.handler.inFlightBytes = retried.getRoughSize();
        final long now = System.nanoTime();

        fixture.handler.updateApplicationLimitedRecovery(now);
        fixture.handler.updateApplicationLimitedRecovery(now + 1L);

        verify(fixture.ctx, times(1)).write(retried);
        verify(fixture.metrics, times(1)).applicationLimitedRecovery(retried.getRoughSize());
        Assertions.assertEquals(retried.getRoughSize(),
                fixture.handler.additionalRecoveryBytesInFlight);

        // Leaving application-limited state starts a new period, but the per-logical
        // RTT cooldown still prevents a back-to-back duplicate.
        fixture.handler.queuedBytes = 1;
        fixture.handler.updateApplicationLimitedRecovery(now + 2L);
        fixture.handler.queuedBytes = 0;
        fixture.handler.updateApplicationLimitedRecovery(now + fixture.rttNanos);
        verify(fixture.ctx, times(2)).write(retried);

        // Release the two outbound retains and the pending ownership.
        retried.release(2);
        fixture.handler.pendingFrameSets.remove(60).release();
    }

    @Test
    public void applicationLimitedRecoveryDoesNotDuplicatePartiallyProtectedFrameSet() {
        final Fixture fixture = new Fixture();
        final FrameSet retried = reliableFrameSet(61, true, 3, true);
        addReliableFrame(retried, 62, true, 3, true);
        fixture.handler.pendingFrameSets.put(61, retried);
        fixture.handler.additionallyRecoveredThisPeriod.add(61);

        Assertions.assertNull(fixture.handler.selectAdditionalRecoveryCandidate(
                System.nanoTime(), fixture.rttNanos));

        fixture.handler.pendingFrameSets.remove(61).release();
    }

    @Test
    public void targetedFecDebtRequiresRetriesAndEnforcesOneMtuPerRttBudget() {
        final Fixture fixture = new Fixture();
        when(fixture.config.getMTU()).thenReturn(256);
        final FrameSet retried = reliableFrameSet(70, true, 4, true);
        fixture.handler.pendingFrameSets.put(70, retried);
        fixture.handler.targetedFecChannelsByFrameSet.put(70, 4);
        final long now = retried.getSentTime() + fixture.rttNanos;

        Assertions.assertTrue(fixture.handler.recoveryDebtForSequence(70, now) >= 2D);
        Assertions.assertTrue(fixture.handler.tryAcquireTargetedFecBudget(70, 200, now));
        Assertions.assertFalse(fixture.handler.tryAcquireTargetedFecBudget(70, 100, now + 1L));
        Assertions.assertTrue(fixture.handler.tryAcquireTargetedFecBudget(
                70, 200, now + fixture.rttNanos));
        verify(fixture.metrics, atLeastOnce()).recoveryDebt(anyDouble(), eq(4));

        fixture.handler.pendingFrameSets.remove(70).release();
    }

    private static FrameSet reliableFrameSet(int sequenceId, boolean ordered, int orderChannel) {
        return reliableFrameSet(sequenceId, ordered, orderChannel, false);
    }

    private static FrameSet reliableFrameSet(int sequenceId, boolean ordered, int orderChannel,
                                             boolean retried) {
        final FrameSet frameSet = FrameSet.create();
        frameSet.setSeqId(sequenceId);
        addReliableFrame(frameSet, sequenceId, ordered, orderChannel, retried);
        return frameSet;
    }

    private static void addReliableFrame(FrameSet frameSet, int reliableIndex, boolean ordered,
                                         int orderChannel, boolean retried) {
        final FrameData data = FrameData.create(ByteBufAllocator.DEFAULT, 0xFE,
                Unpooled.wrappedBuffer(new byte[]{1}));
        data.setReliability(ordered ? FramedPacket.Reliability.RELIABLE_ORDERED
                : FramedPacket.Reliability.RELIABLE);
        data.setOrderChannel(orderChannel);
        final Frame frame = ordered ? Frame.createOrdered(data, reliableIndex, reliableIndex) : Frame.create(data);
        data.release();
        frame.setReliableIndex(reliableIndex);
        if (retried) frame.incRetryCount();
        frameSet.addPacket(frame);
    }

    private static final class Fixture {
        final long rttNanos = TimeUnit.MILLISECONDS.toNanos(80);
        final TestReliabilityHandler handler = new TestReliabilityHandler();
        final RakNet.Config config = mock(RakNet.Config.class);
        final RakNet.MetricsLogger metrics = mock(RakNet.MetricsLogger.class);
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        final Channel channel = mock(Channel.class);
        final EventLoop eventLoop = mock(EventLoop.class);
        final EventExecutor executor = mock(EventExecutor.class);
        final ChannelFuture writeFuture = mock(ChannelFuture.class);
        final ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        final AdaptiveTransportController adaptive;

        Fixture() {
            when(config.getMetrics()).thenReturn(metrics);
            when(config.getRTTNanos()).thenReturn(rttNanos);
            when(config.getRTTStdDevNanos()).thenReturn(TimeUnit.MILLISECONDS.toNanos(2));
            when(config.getDefaultPendingFrameSets()).thenReturn(32);
            when(config.isAutoRead()).thenReturn(true);
            when(ctx.channel()).thenReturn(channel);
            when(ctx.executor()).thenReturn(executor);
            when(ctx.write(any())).thenReturn(writeFuture);
            when(writeFuture.addListener(any())).thenReturn(writeFuture);
            when(channel.isOpen()).thenReturn(true);
            when(channel.eventLoop()).thenReturn(eventLoop);
            doReturn(scheduledFuture).when(eventLoop)
                    .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS));
            handler.config = config;
            handler.ctx = ctx;
            adaptive = new AdaptiveTransportController(config);
            handler.adaptive = adaptive;
        }

        void enableNacks() {
            when(config.isNACKEnabled()).thenReturn(true);
        }

        void activateAckProtection() {
            final long now = System.nanoTime();
            handler.adaptiveAckProtection.onDuplicateFrameSet(now, rttNanos);
            handler.adaptiveAckProtection.onDuplicateFrameSet(now + 1L, rttNanos);
            handler.adaptiveAckProtection.onDuplicateFrameSet(now + 2L, rttNanos);
        }
    }

    private static final class TestReliabilityHandler extends ReliabilityHandler {
        int rackRecalls;

        @Override
        protected void recallFrameSet(FrameSet frameSet) {
            rackRecalls++;
            frameSet.release();
        }
    }
}
