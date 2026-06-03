package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.utils.Constants;
import network.ycc.raknet.utils.UINT;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCountUtil;

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
            channels[frame.getOrderChannel()].decodeOrdered(frame, list, RakNet.config(ctx).getRTTNanos());
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
            final int indexDiff = UINT.B3.minusWrap(frame.getOrderIndex(), lastOrderIndex);
            Constants.packetLossCheck(indexDiff, "ordered difference");
            if (indexDiff == 1) { //got next packet in line
                gapStartNanos = 0; // gap resolved
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

        // Item 2: skip the missing packet and deliver all consecutive queued data
        private void flushGap(List<Object> list) {
            final int gapIdx = UINT.B3.plus(lastOrderIndex, 1);
            FramedPacket data = queue.remove(gapIdx);
            if (data != null) {
                // Gap packet arrived between timeout check and now — deliver normally
                list.add(data);
                lastOrderIndex = gapIdx;
                int nextIdx = UINT.B3.plus(gapIdx, 1);
                while ((data = queue.remove(nextIdx)) != null) {
                    list.add(data);
                    lastOrderIndex = nextIdx;
                    nextIdx = UINT.B3.plus(nextIdx, 1);
                }
            } else {
                // Gap still missing — skip it and deliver the rest
                lastOrderIndex = gapIdx; // accept the gap
                int nextIdx = UINT.B3.plus(gapIdx, 1);
                while ((data = queue.remove(nextIdx)) != null) {
                    list.add(data);
                    lastOrderIndex = nextIdx;
                    nextIdx = UINT.B3.plus(nextIdx, 1);
                }
            }
            gapStartNanos = 0;
        }

        protected void clear() {
            queue.values().forEach(ReferenceCountUtil::release);
            queue.clear();
        }

    }

}
