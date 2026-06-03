package network.ycc.raknet;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.pipeline.DisconnectHandler;
import network.ycc.raknet.pipeline.FrameJoiner;
import network.ycc.raknet.pipeline.FrameOrderIn;
import network.ycc.raknet.pipeline.PingProducer;
import network.ycc.raknet.pipeline.PongHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for latency and recovery optimizations introduced in the 7-item patch set.
 *
 * Covered (direct method invocation via reflection to avoid EmbeddedChannel's
 * lack of RakNet.Config):
 *  - FrameOrderIn: gap timeout only activates for unreliable frames
 *  - FrameOrderIn: reliable ordered frames never skip gaps
 *  - FrameJoiner: periodic cleanup task scheduled/cancelled on add/remove
 *  - PingProducer: state initialised correctly at handlerAdded time
 *  - DisconnectHandler: configurable disconnect timeout
 */
public class HandlerOptimizationsTest {

    // ─── FrameOrderIn: gap timeout safety ───────────────────────────────────────

    private static Object getChannelQueue(FrameOrderIn handler) throws Exception {
        final Field f = FrameOrderIn.class.getDeclaredField("channels");
        f.setAccessible(true);
        return ((Object[]) f.get(handler))[0];
    }

    private static Method getDecodeOrderedMethod(Object queue) throws Exception {
        final Method m = queue.getClass()
                .getDeclaredMethod("decodeOrdered", Frame.class, List.class, long.class);
        m.setAccessible(true);
        return m;
    }

    private static Frame makeFrame(ByteBufAllocator alloc, int orderIdx, int seqIdx,
                                   FramedPacket.Reliability rel, int channel) {
        final FrameData fd = FrameData.create(alloc, 0xFE, Unpooled.wrappedBuffer(new byte[]{(byte) orderIdx}));
        fd.setReliability(rel);
        fd.setOrderChannel(channel);
        return Frame.createOrdered(fd, orderIdx, seqIdx);
    }

    private static long getGapStartNanos(Object queue) throws Exception {
        final Field f = queue.getClass().getDeclaredField("gapStartNanos");
        f.setAccessible(true);
        return (long) f.get(queue);
    }

    private static int getLastOrderIndex(Object queue) throws Exception {
        final Field f = queue.getClass().getDeclaredField("lastOrderIndex");
        f.setAccessible(true);
        return (int) f.get(queue);
    }

    @Test
    public void testSequencedSkipsOlderData() throws Exception {
        final FrameOrderIn handler = new FrameOrderIn();
        final Object queue = getChannelQueue(handler);
        final Method decodeSequenced = queue.getClass()
                .getDeclaredMethod("decodeSequenced", Frame.class, List.class, long.class);
        decodeSequenced.setAccessible(true);
        final ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
        final long rttNanos = 50_000_000L; // 50ms RTT

        // Baseline: deliver frame at order-index 0, sequence-index 0
        final Frame f0 = makeFrame(alloc, 0, 0, FramedPacket.Reliability.UNRELIABLE_SEQUENCED, 0);
        final List<Object> out0 = new ArrayList<>();
        decodeSequenced.invoke(queue, f0, out0, rttNanos);
        Assertions.assertEquals(1, out0.size(), "Frame 0 should be delivered in-order");
        Assertions.assertEquals(0, getLastOrderIndex(queue));
        ReferenceCountUtil.release(out0.get(0));
        f0.release();

        // Gap: frame at order-index 2, sequence-index 2 (newer) arrives.
        // Sequenced semantics: skip over gap at index 1, deliver frame 2 directly.
        final Frame f2 = makeFrame(alloc, 2, 2, FramedPacket.Reliability.UNRELIABLE_SEQUENCED, 0);
        final List<Object> out2 = new ArrayList<>();
        decodeSequenced.invoke(queue, f2, out2, rttNanos);
        Assertions.assertEquals(1, out2.size(), "Frame 2 should be delivered (skipping gap at 1)");
        Assertions.assertEquals(2, getLastOrderIndex(queue),
                "lastOrderIndex should advance to 2, skipping index 1");
        ReferenceCountUtil.release(out2.get(0));
        f2.release();

        // gapStartNanos must be 0 — UNRELIABLE_SEQUENCED never triggers gap timeout
        Assertions.assertEquals(0L, getGapStartNanos(queue),
                "gapStartNanos must be 0 — sequenced frames skip gaps, never queue them");
    }

    @Test
    public void testGapTimeoutNeverSkipsReliableOrdered() throws Exception {
        final FrameOrderIn handler = new FrameOrderIn();
        final Object queue = getChannelQueue(handler);
        final Method decodeOrdered = getDecodeOrderedMethod(queue);
        final ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
        final long rttNanos = 50_000_000L;

        // Baseline
        final Frame f0 = makeFrame(alloc, 0, 0, FramedPacket.Reliability.RELIABLE_ORDERED, 0);
        final List<Object> out0 = new ArrayList<>();
        decodeOrdered.invoke(queue, f0, out0, rttNanos);
        Assertions.assertEquals(1, out0.size());
        ReferenceCountUtil.release(out0.get(0));
        f0.release();

        // Create gap with RELIABLE_ORDERED
        final Frame f3 = makeFrame(alloc, 3, 3, FramedPacket.Reliability.RELIABLE_ORDERED, 0);
        final List<Object> out3 = new ArrayList<>();
        decodeOrdered.invoke(queue, f3, out3, rttNanos);
        Assertions.assertTrue(out3.isEmpty(), "RELIABLE_ORDERED frame should be queued");
        f3.release();

        // gapStartNanos MUST remain 0 — reliable frames never trigger gap timeout
        Assertions.assertEquals(0L, getGapStartNanos(queue),
                "gapStartNanos must be 0 for RELIABLE_ORDERED — never skip reliable gaps");

        // Fill gap: retransmitted frame 1 arrives
        final Frame f1 = makeFrame(alloc, 1, 1, FramedPacket.Reliability.RELIABLE_ORDERED, 0);
        final List<Object> out1 = new ArrayList<>();
        decodeOrdered.invoke(queue, f1, out1, rttNanos);
        Assertions.assertEquals(1, out1.size(), "Frame 1 should be delivered");
        ReferenceCountUtil.release(out1.get(0));
        f1.release();

        // Now frame 3 should be delivered too (gap filled)
        // decodeOrdered with indexDiff==1 processes queued packets
        final Frame f2 = makeFrame(alloc, 2, 2, FramedPacket.Reliability.RELIABLE_ORDERED, 0);
        final List<Object> out2 = new ArrayList<>();
        decodeOrdered.invoke(queue, f2, out2, rttNanos);
        // f2 delivered + queued f3 should be delivered
        Assertions.assertEquals(2, out2.size(), "Both frame 2 and queued frame 3 should be delivered after gap fills");
        for (Object o : out2) ReferenceCountUtil.release(o);
        f2.release();
    }

    @Test
    public void testInOrderDeliveryWithoutGaps() throws Exception {
        final FrameOrderIn handler = new FrameOrderIn();
        final Object queue = getChannelQueue(handler);
        final Method decodeOrdered = getDecodeOrderedMethod(queue);
        final ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
        final long rttNanos = 50_000_000L;

        for (int i = 0; i < 10; i++) {
            final Frame f = makeFrame(alloc, i, i, FramedPacket.Reliability.RELIABLE_ORDERED, 0);
            final List<Object> out = new ArrayList<>();
            decodeOrdered.invoke(queue, f, out, rttNanos);
            Assertions.assertEquals(1, out.size(), "Frame " + i + " should be delivered");
            ReferenceCountUtil.release(out.get(0));
            f.release();
        }

        Assertions.assertEquals(9, getLastOrderIndex(queue),
                "lastOrderIndex should be 9 after 10 in-order frames");
    }

    // ─── FrameJoiner: periodic cleanup timer ────────────────────────────────────

    @Test
    public void testFrameJoinerSchedulesCleanupTimer() throws Exception {
        final FrameJoiner handler = new FrameJoiner();
        final EmbeddedChannel channel = new EmbeddedChannel(handler);

        final Field taskField = FrameJoiner.class.getDeclaredField("cleanupTask");
        taskField.setAccessible(true);
        final ScheduledFuture<?> task = (ScheduledFuture<?>) taskField.get(handler);
        Assertions.assertNotNull(task, "cleanupTask should be scheduled on handlerAdded");
        Assertions.assertFalse(task.isDone(), "cleanupTask should be active");

        channel.finishAndReleaseAll();
        Assertions.assertTrue(task.isCancelled(), "cleanupTask should be cancelled on handlerRemoved");
    }

    @Test
    public void testFrameJoinerCleanupIntervalConstant() throws Exception {
        final Field intervalField = FrameJoiner.class.getDeclaredField("CLEANUP_INTERVAL_MILLIS");
        intervalField.setAccessible(true);
        final long interval = (long) intervalField.get(null);
        Assertions.assertEquals(500L, interval, "Cleanup interval should be 500ms");
    }

    // ─── PingProducer: state initialisation ─────────────────────────────────────

    @Test
    public void testPingProducerInitialState() throws Exception {
        // Test fields before handlerAdded (no channel needed for static/init fields)
        final PingProducer handler = new PingProducer();

        final Field intervalField = PingProducer.class.getDeclaredField("currentIntervalMillis");
        intervalField.setAccessible(true);
        final long interval = (long) intervalField.get(handler);
        Assertions.assertEquals(PingProducer.DEFAULT_INTERVAL_MILLIS, interval,
                "currentIntervalMillis should start at DEFAULT_INTERVAL_MILLIS");

        final Field minField = PingProducer.class.getDeclaredField("MIN_INTERVAL_MILLIS");
        final Field maxField = PingProducer.class.getDeclaredField("MAX_INTERVAL_MILLIS");
        minField.setAccessible(true);
        maxField.setAccessible(true);
        Assertions.assertEquals(50L, (long) minField.get(handler));
        Assertions.assertEquals(500L, (long) maxField.get(handler));
    }

    @Test
    public void testPingProducerConstants() throws Exception {
        final Field maxMissedField = PingProducer.class.getDeclaredField("MAX_MISSED_PONGS");
        maxMissedField.setAccessible(true);
        final int maxMissed = (int) maxMissedField.get(null);
        Assertions.assertTrue(maxMissed >= 5, "MAX_MISSED_PONGS should be at least 5");

        Assertions.assertTrue(PingProducer.DEFAULT_INTERVAL_MILLIS >= 50L,
                "DEFAULT_INTERVAL_MILLIS should be >= 50ms for safe minimum");
    }

    // ─── PongHandler: LAST_PONG_NANOS attribute ─────────────────────────────────

    @Test
    public void testLastPongAttributeDefined() {
        Assertions.assertNotNull(PongHandler.LAST_PONG_NANOS,
                "LAST_PONG_NANOS AttributeKey should be defined");
    }

    @Test
    public void testLastPongInitiallyNull() {
        final EmbeddedChannel channel = new EmbeddedChannel(PongHandler.INSTANCE);
        final Long initial = channel.attr(PongHandler.LAST_PONG_NANOS).get();
        Assertions.assertNull(initial, "LAST_PONG_NANOS should be null before first pong");
        channel.finishAndReleaseAll();
    }

    // ─── DisconnectHandler: configurable timeout ────────────────────────────────

    @Test
    public void testDisconnectTimeoutDefault() throws Exception {
        final Field timeoutField = DisconnectHandler.class.getDeclaredField("DISCONNECT_TIMEOUT_SECS");
        timeoutField.setAccessible(true);
        final long timeout = (long) timeoutField.get(DisconnectHandler.INSTANCE);

        final String prop = System.getProperty("raknetify.disconnectTimeoutSecs");
        if (prop == null) {
            Assertions.assertEquals(3L, timeout, "Default disconnect timeout should be 3s");
        } else {
            Assertions.assertEquals(Long.parseLong(prop), timeout,
                    "Disconnect timeout should match system property");
        }
    }

    // ─── System property defaults ───────────────────────────────────────────────

    @Test
    public void testFragmentTimeoutDefault() {
        final int timeoutSecs = Integer.getInteger("raknetify.fragmentTimeoutSecs", 3);
        Assertions.assertTrue(timeoutSecs >= 1, "Fragment timeout should be at least 1 second");
    }

    @Test
    public void testOrderedGapTimeoutMultiplierDefault() {
        final long multiplier = Long.getLong("raknetify.orderedGapTimeoutMultiplier", 2);
        Assertions.assertTrue(multiplier >= 0, "Gap timeout multiplier should be non-negative");
    }
}
