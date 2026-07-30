package network.ycc.raknet.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import network.ycc.raknet.packet.FramedPacket;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FramePriorityTest {

    @Test
    void higherPriorityReliableFrameCanPreemptOlderBulkFrame() {
        final Frame olderBulk = frame(10, 1);
        final Frame newerStrict = frame(11, 2);
        final Frame newestControl = frame(12, 4);
        final PriorityQueue<Frame> queue = new PriorityQueue<>(Frame.COMPARATOR);
        queue.add(olderBulk);
        queue.add(newerStrict);
        queue.add(newestControl);

        assertEquals(newestControl, queue.poll());
        assertEquals(newerStrict, queue.poll());
        assertEquals(olderBulk, queue.poll());

        newestControl.release();
        newerStrict.release();
        olderBulk.release();
    }

    @Test
    void equalPriorityRetainsReliableIndexOrder() {
        final Frame first = frame(20, 2);
        final Frame second = frame(21, 2);
        final PriorityQueue<Frame> queue = new PriorityQueue<>(Frame.COMPARATOR);
        queue.add(second);
        queue.add(first);

        assertEquals(first, queue.poll());
        assertEquals(second, queue.poll());

        first.release();
        second.release();
    }

    @Test
    void fragmentationPreservesSenderLocalPriority() {
        final ByteBuf payload = UnpooledByteBufAllocator.DEFAULT.buffer(256)
                .writeZero(256);
        final FrameData data = FrameData.read(
                payload,
                payload.readableBytes(),
                false
        );
        payload.release();
        data.setReliability(FramedPacket.Reliability.RELIABLE_ORDERED);
        data.setOrderChannel(6);
        data.setPriority(3);
        final Frame frame = Frame.createOrdered(data, 0, 0);
        data.release();
        final List<Object> fragments = new ArrayList<>();

        frame.fragment(1, 96, 0, fragments);

        for (Object value : fragments) {
            final Frame fragment = (Frame) value;
            final FrameData fragmentData = fragment.retainedFrameData();
            try {
                assertEquals(3, fragmentData.getPriority());
            } finally {
                fragmentData.release();
                fragment.release();
            }
        }
        frame.release();
    }

    private static Frame frame(int reliableIndex, int priority) {
        final ByteBuf payload = UnpooledByteBufAllocator.DEFAULT.buffer(1)
                .writeByte(1);
        final FrameData data = FrameData.read(
                payload,
                payload.readableBytes(),
                false
        );
        payload.release();
        data.setReliability(FramedPacket.Reliability.RELIABLE_ORDERED);
        data.setPriority(priority);
        final Frame frame = Frame.createOrdered(data, reliableIndex, 0);
        frame.setReliableIndex(reliableIndex);
        data.release();
        return frame;
    }
}
