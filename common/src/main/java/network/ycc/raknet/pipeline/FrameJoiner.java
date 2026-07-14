package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.utils.Constants;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.List;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

public class FrameJoiner extends MessageToMessageDecoder<Frame> {

    public static final String NAME = "rn-join";

    protected final Int2ObjectOpenHashMap<Builder> pendingPackets = new Int2ObjectOpenHashMap<>();
    protected long lastCleanupNanos = 0;
    protected long totalPendingBytes = 0;
    protected long lastMetricsPublishNanos = 0;
    protected ScheduledFuture<?> cleanupTask = null;
    private static final long CLEANUP_INTERVAL_MILLIS = 500;
    private static final long CLEANUP_INTERVAL_NANOS = TimeUnit.NANOSECONDS.convert(CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    private static final int DEFAULT_MAX_PENDING_BUILDERS = 256;
    private static final long DEFAULT_MAX_PENDING_BYTES = 4L * 1024L * 1024L; // 4 MiB
    private static final long DEFAULT_RELIABLE_TIMEOUT_SECS = 300L;
    private static final long DEFAULT_ABSOLUTE_TIMEOUT_SECS = 600L;
    private static final int DEFAULT_MAX_DUPLICATE_FRAGMENTS = 64;
    private static final int MAX_FRAGMENT_COMPONENTS = 16384;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        cleanupTask = ctx.channel().eventLoop().scheduleAtFixedRate(
                () -> cleanupExpired(ctx),
                CLEANUP_INTERVAL_MILLIS, CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        if (cleanupTask != null) {
            cleanupTask.cancel(false);
            cleanupTask = null;
        }
        super.handlerRemoved(ctx);
        pendingPackets.values().forEach(Builder::release);
        pendingPackets.clear();
        totalPendingBytes = 0;
        final RakNet.MetricsLogger metrics = metrics(ctx);
        if (metrics != null) metrics.fragmentReassemblyPending(0, 0, 0);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, Frame frame, List<Object> list) {
        cleanupExpired(ctx);
        if (!frame.hasSplit()) {
            frame.touch("Not split");
            list.add(frame.retain());
        } else {
            final int splitID = frame.getSplitId();
            final Builder partial = pendingPackets.get(splitID);
            final int splitCount = frame.getSplitCount();
            final long totalSize = (long) splitCount * (long) frame.getRoughPacketSize();
            frame.touch("Is split");
            if (splitCount <= 0 || frame.getSplitIndex() < 0 || frame.getSplitIndex() >= splitCount) {
                throw new CorruptedFrameException("Invalid split: count=" + splitCount + " index=" + frame.getSplitIndex());
            }
            if (splitCount > MAX_FRAGMENT_COMPONENTS) {
                throw new TooLongFrameException("Fragmented frame split count exceeds maximum: " + splitCount);
            }
            if (totalSize > Integer.MAX_VALUE) {
                throw new TooLongFrameException("Fragmented frame total size exceeds maximum");
            }
            if (totalSize > RakNet.config(ctx).getMaxQueuedBytes()) {
                throw new TooLongFrameException("Fragmented frame too large");
            }
            if (partial == null) {
                Constants.packetLossCheck(splitCount, "frame join elements");
                final int maxPendingBuilders = positiveIntProperty("raknetify.maxPendingBuilders", DEFAULT_MAX_PENDING_BUILDERS);
                if (pendingPackets.size() >= maxPendingBuilders) {
                    final String msg = "Pending fragment builders exceeded: " + pendingPackets.size() + " >= " + maxPendingBuilders;
                    final CodecException e = new CodecException(msg);
                    ctx.close();
                    throw e;
                }
                final Builder builder = Builder.create(ctx.alloc(), frame);
                if (builder.isDone()) {
                    final int bytes = Math.toIntExact(builder.actualBytes);
                    final long age = System.nanoTime() - builder.firstCreatedAt;
                    list.add(builder.finish());
                    final RakNet.MetricsLogger metrics = metrics(ctx);
                    if (metrics != null) metrics.fragmentReassemblyComplete(bytes, age);
                } else {
                    pendingPackets.put(splitID, builder);
                    totalPendingBytes += builder.actualBytes;
                }
            } else {
                partial.validate(frame);
                final long before = partial.actualBytes;
                partial.add(frame);
                totalPendingBytes += partial.actualBytes - before;
                if (partial.isDone()) {
                    pendingPackets.remove(splitID);
                    totalPendingBytes -= partial.actualBytes;
                    final int bytes = Math.toIntExact(partial.actualBytes);
                    final long age = System.nanoTime() - partial.firstCreatedAt;
                    list.add(partial.finish());
                    final RakNet.MetricsLogger metrics = metrics(ctx);
                    if (metrics != null) metrics.fragmentReassemblyComplete(bytes, age);
                }
            }
            Constants.packetLossCheck(pendingPackets.size(), "pending frame joins");
            publishMetrics(ctx, false);
        }
    }

    private void cleanupExpired(ChannelHandlerContext ctx) {
        final long now = System.nanoTime();
        if (now - lastCleanupNanos < CLEANUP_INTERVAL_NANOS) return;
        lastCleanupNanos = now;

        final long unreliableTimeoutNanos = TimeUnit.NANOSECONDS.convert(
                positiveIntProperty("raknetify.fragmentTimeoutSecs", 3), TimeUnit.SECONDS);
        final long reliableTimeoutNanos = TimeUnit.NANOSECONDS.convert(
                positiveLongProperty("raknetify.reliableFragmentTimeoutSecs", DEFAULT_RELIABLE_TIMEOUT_SECS), TimeUnit.SECONDS);
        final long absoluteTimeoutNanos = TimeUnit.NANOSECONDS.convert(
                positiveLongProperty("raknetify.absoluteFragmentTimeoutSecs", DEFAULT_ABSOLUTE_TIMEOUT_SECS), TimeUnit.SECONDS);
        final int maxPendingBuilders = positiveIntProperty("raknetify.maxPendingBuilders", DEFAULT_MAX_PENDING_BUILDERS);
        final long maxPendingBytes = positiveLongProperty("raknetify.maxPendingFragmentBytes", DEFAULT_MAX_PENDING_BYTES);

        // Guard against remote memory exhaustion: close the connection
        // rather than silently dropping reliable fragments (which would
        // cause permanent data loss since ACKed fragments aren't retransmitted).
        if (totalPendingBytes > maxPendingBytes) {
            final String msg = "Pending fragment bytes exceeded: " + totalPendingBytes + " > " + maxPendingBytes;
            final CodecException e = new CodecException(msg);
            ctx.close();
            throw e;
        }
        if (pendingPackets.size() > maxPendingBuilders) {
            final String msg = "Pending fragment builders exceeded: " + pendingPackets.size() + " > " + maxPendingBuilders;
            final CodecException e = new CodecException(msg);
            ctx.close();
            throw e;
        }

        final ObjectIterator<Int2ObjectMap.Entry<Builder>> it = pendingPackets.int2ObjectEntrySet().fastIterator();
        while (it.hasNext()) {
            final Int2ObjectMap.Entry<Builder> entry = it.next();
            final Builder builder = entry.getValue();
            if (builder.isAbsolutelyExpired(absoluteTimeoutNanos)) {
                totalPendingBytes -= builder.actualBytes;
                builder.release();
                it.remove();
                ctx.close();
                throw new CorruptedFrameException("Fragment reassembly exceeded absolute lifetime");
            }
            if (builder.reliability.isReliable) {
                // All fragments are forced to reliable (Frame.java:166).
                if (builder.isExpired(reliableTimeoutNanos)) {
                    totalPendingBytes -= builder.actualBytes;
                    builder.release();
                    it.remove();
                    ctx.close();
                    throw new CorruptedFrameException(
                            "Reliable fragment reassembly timed out after " +
                                    TimeUnit.NANOSECONDS.toSeconds(reliableTimeoutNanos) + "s");
                }
                continue;
            }
            if (builder.isExpired(unreliableTimeoutNanos)) {
                totalPendingBytes -= builder.actualBytes;
                builder.release();
                it.remove();
            }
        }
        publishMetrics(ctx, true);
    }

    private void publishMetrics(ChannelHandlerContext ctx, boolean force) {
        final long now = System.nanoTime();
        if (!force && now - lastMetricsPublishNanos < TimeUnit.MILLISECONDS.toNanos(100)) return;
        lastMetricsPublishNanos = now;
        long oldestCreatedAt = now;
        for (Builder builder : pendingPackets.values()) {
            oldestCreatedAt = Math.min(oldestCreatedAt, builder.firstCreatedAt);
        }
        final long oldestAge = pendingPackets.isEmpty() ? 0 : Math.max(0, now - oldestCreatedAt);
        final RakNet.MetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.fragmentReassemblyPending(pendingPackets.size(), totalPendingBytes, oldestAge);
        }
    }

    private static RakNet.MetricsLogger metrics(ChannelHandlerContext ctx) {
        return ctx.channel().config() instanceof RakNet.Config
                ? ((RakNet.Config) ctx.channel().config()).getMetrics() : null;
    }

    private static int positiveIntProperty(String name, int fallback) {
        final int value = Integer.getInteger(name, fallback);
        return value > 0 ? value : fallback;
    }

    private static long positiveLongProperty(String name, long fallback) {
        final long value = Long.getLong(name, fallback);
        return value > 0 ? value : fallback;
    }

    protected static final class Builder {

        protected final Int2ObjectOpenHashMap<ByteBuf> queue;
        protected Frame samplePacket;
        protected CompositeByteBuf data;
        protected int splitIdx;
        protected int orderId;
        protected FrameData.Reliability reliability;
        protected long createdAt;
        protected long firstCreatedAt;
        protected int duplicateFragments;
        protected long estimatedBytes;
        protected long actualBytes;

        private Builder(int size) {
            queue = new Int2ObjectOpenHashMap<>(size);
        }

        private static Builder create(ByteBufAllocator alloc, Frame frame) {
            final int cappedSplitCount = Math.min(frame.getSplitCount(), MAX_FRAGMENT_COMPONENTS);
            final Builder out = new Builder(cappedSplitCount);
            out.init(alloc, frame, cappedSplitCount);
            out.createdAt = System.nanoTime();
            out.firstCreatedAt = out.createdAt;
            return out;
        }

        void init(ByteBufAllocator alloc, Frame packet, int cappedComponents) {
            assert data == null;
            splitIdx = 0;
            data = alloc.compositeDirectBuffer(cappedComponents);
            orderId = packet.getOrderChannel();
            reliability = packet.getReliability();
            estimatedBytes = (long) packet.getSplitCount() * (long) packet.getRoughPacketSize();
            samplePacket = packet.retain();
            add(packet);
        }

        boolean isExpired(long timeoutNanos) {
            return System.nanoTime() - createdAt > timeoutNanos;
        }

        boolean isAbsolutelyExpired(long timeoutNanos) {
            return System.nanoTime() - firstCreatedAt > timeoutNanos;
        }

        void add(Frame packet) {
            assert packet.getReliability().equals(samplePacket.getReliability());
            assert packet.getOrderChannel() == samplePacket.getOrderChannel();
            assert packet.getOrderIndex() == samplePacket.getOrderIndex();
            if (!queue.containsKey(packet.getSplitIndex()) && packet.getSplitIndex() >= splitIdx) {
                final ByteBuf fragmentData = packet.retainedFragmentData();
                queue.put(packet.getSplitIndex(), fragmentData);
                actualBytes += fragmentData.readableBytes();
                createdAt = System.nanoTime(); // refresh activity time
                update();
            } else if (++duplicateFragments > positiveIntProperty(
                    "raknetify.maxDuplicateFragments", DEFAULT_MAX_DUPLICATE_FRAGMENTS)) {
                throw new CorruptedFrameException("Too many duplicate fragments for split ID " + packet.getSplitId());
            }
            Constants.packetLossCheck(queue.size(), "packet defragment queue");
        }

        void validate(Frame packet) {
            if (packet.getSplitCount() != samplePacket.getSplitCount()
                    || !packet.getReliability().equals(reliability)
                    || packet.getOrderChannel() != orderId
                    || packet.getOrderIndex() != samplePacket.getOrderIndex()) {
                throw new CorruptedFrameException("Conflicting fragments share split ID " + packet.getSplitId());
            }
        }

        void update() {
            ByteBuf fragment;
            while ((fragment = queue.remove(splitIdx)) != null) {
                data.addComponent(true, fragment);
                splitIdx++;
            }
        }

        Frame finish() {
            assert isDone();
            assert queue.isEmpty();
            try {
                return samplePacket.completeFragment(data);
            } finally {
                release();
            }
        }

        boolean isDone() {
            assert samplePacket.getSplitCount() >= splitIdx;
            return samplePacket.getSplitCount() == splitIdx;
        }

        void release() {
            if (data != null) {
                data.release();
                data = null;
            }
            if (samplePacket != null) {
                samplePacket.release();
                samplePacket = null;
            }
            queue.values().forEach(ReferenceCountUtil::release);
            queue.clear();
        }

    }

}
