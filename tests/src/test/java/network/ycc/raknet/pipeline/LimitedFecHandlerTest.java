package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class LimitedFecHandlerTest {

    @Test
    public void lowYieldNormalFecIsSuppressedAfterAFeedbackWindow() {
        Assertions.assertFalse(LimitedFecHandler.shouldSuppressNormalFec(63, 0));
        Assertions.assertTrue(LimitedFecHandler.shouldSuppressNormalFec(64, 0));
        Assertions.assertTrue(LimitedFecHandler.shouldSuppressNormalFec(100, 0));
        Assertions.assertFalse(LimitedFecHandler.shouldSuppressNormalFec(100, 1));
    }

    @Test
    public void targetedWindowPinsCriticalSequenceAndUsesNewestContext() {
        final java.util.List<FecPacketCodec.Entry> rolling = new java.util.ArrayList<>();
        for (int sequence = 10; sequence <= 15; sequence++) {
            rolling.add(new FecPacketCodec.Entry(sequence, 4, new byte[]{1, 2, 3, 4}));
        }
        final java.util.List<FecPacketCodec.Entry> selected =
                LimitedFecHandler.selectTargetedGroup(rolling, 11);
        Assertions.assertEquals(4, selected.size());
        Assertions.assertEquals(11, selected.get(0).seq);
        Assertions.assertEquals(java.util.Arrays.asList(15, 14, 13),
                selected.subList(1, 4).stream().map(entry -> entry.seq)
                        .collect(java.util.stream.Collectors.toList()));
    }
    @Test
    public void recoversAnySingleMissingPacketWithUnequalLengths() {
        final List<byte[]> packets = Arrays.asList(
                new byte[]{1, 2, 3},
                new byte[]{4, 5, 6, 7, 8},
                new byte[]{9},
                new byte[]{10, 11, 12, 13});
        final byte[] parity = FecPacketCodec.xor(packets);
        for (int missing = 0; missing < packets.size(); missing++) {
            final java.util.ArrayList<byte[]> present = new java.util.ArrayList<>(packets);
            final byte[] expected = present.remove(missing);
            Assertions.assertArrayEquals(expected,
                    FecPacketCodec.recover(parity, present, expected.length));
        }
    }

    @Test
    public void parityIsIndependentOfPacketOrder() {
        final byte[] a = {1, 3, 5};
        final byte[] b = {2, 4};
        Assertions.assertArrayEquals(
                FecPacketCodec.xor(Arrays.asList(a, b)),
                FecPacketCodec.xor(Arrays.asList(b, a)));
    }

    @Test
    public void adaptsGroupSizeToMeasuredRandomLoss() {
        Assertions.assertEquals(8, LimitedFecHandler.selectGroupSize(0.01D));
        Assertions.assertEquals(6, LimitedFecHandler.selectGroupSize(0.04D));
        Assertions.assertEquals(4, LimitedFecHandler.selectGroupSize(0.08D));
    }

    @Test
    public void reedSolomonRecoversTwoMissingUnequalShards() {
        final List<byte[]> packets = Arrays.asList(
                new byte[]{1, 2, 3, 4}, new byte[]{5, 6}, new byte[]{7, 8, 9},
                new byte[]{10, 11, 12, 13, 14}, new byte[]{15}, new byte[]{16, 17, 18});
        final byte[][] parity = ReedSolomonCodec.encode(packets, 2);
        final byte[][] data = packets.toArray(new byte[packets.size()][]);
        final int[] lengths = new int[data.length];
        for (int i = 0; i < data.length; i++) lengths[i] = data[i].length;
        data[1] = null;
        data[4] = null;
        final byte[][] recovered = ReedSolomonCodec.recover(data, lengths, parity);
        Assertions.assertNotNull(recovered);
        Assertions.assertArrayEquals(packets.get(1), recovered[0]);
        Assertions.assertArrayEquals(packets.get(4), recovered[1]);
    }

    @Test
    public void reedSolomonRecoversEveryTwoShardCombination() {
        final List<byte[]> packets = Arrays.asList(
                new byte[]{1, 2, 3}, new byte[]{4, 5, 6, 7}, new byte[]{8},
                new byte[]{9, 10}, new byte[]{11, 12, 13, 14, 15}, new byte[]{16, 17});
        final byte[][] parity = ReedSolomonCodec.encode(packets, 2);
        for (int first = 0; first < packets.size(); first++) {
            for (int second = first + 1; second < packets.size(); second++) {
                final byte[][] data = packets.toArray(new byte[packets.size()][]);
                final int[] lengths = new int[data.length];
                for (int i = 0; i < data.length; i++) lengths[i] = data[i].length;
                data[first] = null;
                data[second] = null;
                final byte[][] recovered = ReedSolomonCodec.recover(data, lengths, parity);
                Assertions.assertNotNull(recovered);
                Assertions.assertArrayEquals(packets.get(first), recovered[0]);
                Assertions.assertArrayEquals(packets.get(second), recovered[1]);
            }
        }
    }

    @Test
    public void rejectsLegacyMetadataLongerThanParityShard() throws Exception {
        final ByteBuf packet = Unpooled.buffer();
        packet.writeByte(0x1e).writeInt(1).writeByte(4);
        for (int i = 0; i < 4; i++) packet.writeMediumLE(i).writeShort(5);
        packet.writeShort(4).writeZero(4);
        try {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> FecPacketCodec.readLegacyParity(packet));
        } finally {
            packet.release();
        }
    }
}
