package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.utils.Constants;
import network.ycc.raknet.utils.UINT;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.Arrays;
import java.util.List;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class FrameOrderIn extends MessageToMessageDecoder<Frame> {

    public static final String NAME = "rn-order-in";

    protected final OrderedChannelPacketQueue[] channels = new OrderedChannelPacketQueue[8];

    public FrameOrderIn() {
        for (int i = 0; i < channels.length; i++) {
            channels[i] = new OrderedChannelPacketQueue();
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        Arrays.stream(channels).forEach(OrderedChannelPacketQueue::clear);
    }

    protected void decode(ChannelHandlerContext ctx, Frame frame, List<Object> list) {
        // Item 2: gap timeout = GAP_TIMEOUT_MULTIPLIER * RTT, one retransmission cycle
        if (frame.getReliability().isSequenced) {
            frame.touch("Sequenced");
            channels[frame.getOrderChannel()].decodeSequenced(frame, list, RakNet.config(ctx).getRTTNanos());
        } else if (frame.getReliability().isOrdered) {
            frame.touch("Ordered");
            channels[frame.getOrderChannel()].decodeOrdered(ctx, frame, list, RakNet.config(ctx).getRTTNanos());
        } else {
            frame.touch("No order");
            list.add(frame.retainedFrameData());
        }
    }

    protected static class OrderedChannelPacketQueue {

        protected final Int2ObjectOpenHashMap<FramedPacket> queue = new Int2ObjectOpenHashMap<>();
        protected int lastOrderIndex = -1;
        protected int lastSequenceIndex = -1;

        // Item 2: gap timeout tracking — when a gap is detected, record the time.
        // If the missing packet doesn't arrive within gapTimeoutNanos, flush queued
        // packets to unblock head-of-line. Default: 2x RTT (one retransmission cycle).
        protected long gapStartNanos = 0;
        protected ScheduledFuture<?> gapTask;
        protected boolean gapIsSkippable;
        private static final long GAP_TIMEOUT_MULTIPLIER = Long.getLong("raknetify.orderedGapTimeoutMultiplier", 2);

        protected void decodeSequenced(Frame frame, List<Object> list, long rttNanos) {
            if (UINT.B3.minusWrap(frame.getSequenceIndex(), lastSequenceIndex) > 0) {
                lastSequenceIndex = frame.getSequenceIndex();
                //remove earlier packets from queue
                while (UINT.B3.minusWrap(frame.getOrderIndex(), lastOrderIndex) > 1) {
                    ReferenceCountUtil.release(queue.remove(lastOrderIndex));
                    lastOrderIndex = UINT.B3.plus(lastOrderIndex, 1);
                }
            }
            decodeOrdered(frame, list, rttNanos); //register packet as normal
        }

        protected void decodeOrdered(Frame frame, List<Object> list, long rttNanos) {
            decodeOrdered(null, frame, list, rttNanos);
        }

        protected void decodeOrdered(ChannelHandlerContext ctx, Frame frame, List<Object> list, long rttNanos) {
            final int indexDiff = UINT.B3.minusWrap(frame.getOrderIndex(), lastOrderIndex);
            Constants.packetLossCheck(indexDiff, "ordered difference");
            if (indexDiff == 1) { //got next packet in line
                cancelGapTask();
                FramedPacket data = frame.retainedFrameData();
                do { //process this packet, and any queued packets following in sequence
                    list.add(data);
                    lastOrderIndex = UINT.B3.plus(lastOrderIndex, 1);
                    data = queue.remove(UINT.B3.plus(lastOrderIndex, 1));
                } while (data != null);
            } else if (indexDiff > 1 && !queue.containsKey(frame.getOrderIndex())) {
                // only new future data goes in the queue
                // Gap timeout only applies to unreliable frames — reliable frames
                // will be retransmitted, so skipping them would break RELIABLE_ORDERED.
                final boolean isUnreliable = !frame.getReliability().isReliable;
                if (isUnreliable && gapStartNanos == 0) {
                    gapStartNanos = System.nanoTime();
                    gapIsSkippable = true;
                    if (ctx != null) {
                        final long delay = Math.max(1_000_000L, saturatedMultiply(
                                Math.max(1L, rttNanos), Math.max(1L, GAP_TIMEOUT_MULTIPLIER)));
                        gapTask = ctx.executor().schedule(() -> flushGap(ctx), delay, java.util.concurrent.TimeUnit.NANOSECONDS);
                    }
                } else if (!isUnreliable) {
                    // Never let a timer skip a gap once reliable ordered data is waiting.
                    cancelGapTask();
                }
                queue.put(frame.getOrderIndex(), frame.retainedFrameData());
                if (isUnreliable && gapStartNanos > 0) {
                    final long gapTimeoutNanos = rttNanos * GAP_TIMEOUT_MULTIPLIER;
                    if (System.nanoTime() - gapStartNanos > gapTimeoutNanos && !queue.isEmpty()) {
                        flushGap(list);
                    }
                }
            }
            Constants.packetLossCheck(queue.size(), "missed ordered packets");
        }

        private void flushGap(ChannelHandlerContext ctx) {
            if (!gapIsSkippable || queue.isEmpty() || !ctx.channel().isOpen()) return;
            final java.util.ArrayList<Object> out = new java.util.ArrayList<>();
            flushGap(out);
            for (Object msg : out) ctx.fireChannelRead(msg);
            if (!out.isEmpty()) ctx.fireChannelReadComplete();
        }

        private static long saturatedMultiply(long value, long multiplier) {
            return value > Long.MAX_VALUE / multiplier ? Long.MAX_VALUE : value * multiplier;
        }

        private void cancelGapTask() {
            gapStartNanos = 0;
            gapIsSkippable = false;
            if (gapTask != null) {
                gapTask.cancel(false);
                gapTask = null;
            }
        }

        // Item 2: skip over the missing packet(s) and deliver all queued data
        // that follows the gap. Handles consecutive gaps by scanning forward.
        private void flushGap(List<Object> list) {
            int nextIdx = UINT.B3.plus(lastOrderIndex, 1);
            while (true) {
                FramedPacket data = queue.remove(nextIdx);
                if (data != null) {
                    // Found a packet past the gap(s) — deliver it and continue
                    // delivering everything consecutive that follows
                    list.add(data);
                    lastOrderIndex = nextIdx;
                    nextIdx = UINT.B3.plus(nextIdx, 1);
                    while ((data = queue.remove(nextIdx)) != null) {
                        list.add(data);
                        lastOrderIndex = nextIdx;
                        nextIdx = UINT.B3.plus(nextIdx, 1);
                    }
                    break;
                }
                // This index is also missing — skip it and continue scanning
                lastOrderIndex = nextIdx;
                nextIdx = UINT.B3.plus(nextIdx, 1);
                if (queue.isEmpty()) break;
            }
            gapStartNanos = 0;
            gapIsSkippable = false;
            gapTask = null;
        }

        protected void clear() {
            cancelGapTask();
            queue.values().forEach(ReferenceCountUtil::release);
            queue.clear();
        }

    }

}
