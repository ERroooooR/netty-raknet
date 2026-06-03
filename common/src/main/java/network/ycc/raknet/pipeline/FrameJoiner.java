package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.utils.Constants;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
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
    protected ScheduledFuture<?> cleanupTask = null;
    private static final long CLEANUP_INTERVAL_MILLIS = 500;
    private static final long CLEANUP_INTERVAL_NANOS = TimeUnit.NANOSECONDS.convert(CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);

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
            final int totalSize = frame.getSplitCount() * frame.getRoughPacketSize();
            frame.touch("Is split");
            if (totalSize > RakNet.config(ctx).getMaxQueuedBytes()) {
                throw new TooLongFrameException("Fragmented frame too large");
            } else if (partial == null) {
                Constants.packetLossCheck(frame.getSplitCount(), "frame join elements");
                pendingPackets.put(splitID, Builder.create(ctx.alloc(), frame));
            } else {
                partial.add(frame);
                if (partial.isDone()) {
                    pendingPackets.remove(splitID);
                    list.add(partial.finish());
                }
            }
            Constants.packetLossCheck(pendingPackets.size(), "pending frame joins");
        }
    }

    private void cleanupExpired(ChannelHandlerContext ctx) {
        final long now = System.nanoTime();
        if (now - lastCleanupNanos < CLEANUP_INTERVAL_NANOS) return;
        lastCleanupNanos = now;

        final long unreliableTimeoutNanos = TimeUnit.NANOSECONDS.convert(
                Integer.getInteger("raknetify.fragmentTimeoutSecs", 3), TimeUnit.SECONDS);
        final long reliableTimeoutNanos = TimeUnit.NANOSECONDS.convert(
                Integer.getInteger("raknetify.reliableFragmentTimeoutSecs", 60), TimeUnit.SECONDS);

        final ObjectIterator<Int2ObjectMap.Entry<Builder>> it = pendingPackets.int2ObjectEntrySet().fastIterator();
        while (it.hasNext()) {
            final Int2ObjectMap.Entry<Builder> entry = it.next();
            final Builder builder = entry.getValue();
            final long timeoutNanos = builder.reliability.isReliable ? reliableTimeoutNanos : unreliableTimeoutNanos;
            if (builder.isExpired(timeoutNanos)) {
                builder.release();
                it.remove();
            }
        }
    }

    protected static final class Builder {

        protected final Int2ObjectOpenHashMap<ByteBuf> queue;
        protected Frame samplePacket;
        protected CompositeByteBuf data;
        protected int splitIdx;
        protected int orderId;
        protected FrameData.Reliability reliability;
        protected long createdAt;

        private Builder(int size) {
            queue = new Int2ObjectOpenHashMap<>(size);
        }

        private static Builder create(ByteBufAllocator alloc, Frame frame) {
            final Builder out = new Builder(frame.getSplitCount());
            out.init(alloc, frame);
            out.createdAt = System.nanoTime();
            return out;
        }

        void init(ByteBufAllocator alloc, Frame packet) {
            assert data == null;
            splitIdx = 0;
            data = alloc.compositeDirectBuffer(packet.getSplitCount());
            orderId = packet.getOrderChannel();
            reliability = packet.getReliability();
            samplePacket = packet.retain();
            add(packet);
        }

        boolean isExpired(long timeoutNanos) {
            return System.nanoTime() - createdAt > timeoutNanos;
        }

        void add(Frame packet) {
            assert packet.getReliability().equals(samplePacket.getReliability());
            assert packet.getOrderChannel() == samplePacket.getOrderChannel();
            assert packet.getOrderIndex() == samplePacket.getOrderIndex();
            if (!queue.containsKey(packet.getSplitIndex()) && packet.getSplitIndex() >= splitIdx) {
                queue.put(packet.getSplitIndex(), packet.retainedFragmentData());
                createdAt = System.nanoTime(); // refresh activity time
                update();
            }
            Constants.packetLossCheck(queue.size(), "packet defragment queue");
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
