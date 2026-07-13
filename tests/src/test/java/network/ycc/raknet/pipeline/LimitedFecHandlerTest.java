package network.ycc.raknet.pipeline;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class LimitedFecHandlerTest {
    @Test
    public void recoversAnySingleMissingPacketWithUnequalLengths() {
        final List<byte[]> packets = Arrays.asList(
                new byte[]{1, 2, 3},
                new byte[]{4, 5, 6, 7, 8},
                new byte[]{9},
                new byte[]{10, 11, 12, 13});
        final byte[] parity = LimitedFecHandler.xor(packets);
        for (int missing = 0; missing < packets.size(); missing++) {
            final java.util.ArrayList<byte[]> present = new java.util.ArrayList<>(packets);
            final byte[] expected = present.remove(missing);
            Assertions.assertArrayEquals(expected,
                    LimitedFecHandler.recover(parity, present, expected.length));
        }
    }

    @Test
    public void parityIsIndependentOfPacketOrder() {
        final byte[] a = {1, 3, 5};
        final byte[] b = {2, 4};
        Assertions.assertArrayEquals(
                LimitedFecHandler.xor(Arrays.asList(a, b)),
                LimitedFecHandler.xor(Arrays.asList(b, a)));
    }

    @Test
    public void adaptsGroupSizeToMeasuredRandomLoss() {
        Assertions.assertEquals(8, LimitedFecHandler.selectGroupSize(0.01D));
        Assertions.assertEquals(6, LimitedFecHandler.selectGroupSize(0.04D));
        Assertions.assertEquals(4, LimitedFecHandler.selectGroupSize(0.08D));
    }
}
