package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.CorruptedFrameException;
import network.ycc.raknet.utils.UINT;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReliabilityValidationTest {

    @Test
    public void rejectsOneOversizedAcknowledgementRangeBeforeExpansion() {
        final ByteBuf encoded = Unpooled.buffer()
                .writeShort(1)
                .writeBoolean(false)
                .writeMediumLE(0)
                .writeMediumLE(8192);
        try {
            Assertions.assertThrows(CorruptedFrameException.class,
                    () -> new Reliability.ACK().decode(encoded));
        } finally {
            encoded.release();
        }
    }

    @Test
    public void rejectsExcessiveTotalAcrossIndividuallyValidRanges() {
        final ByteBuf encoded = Unpooled.buffer()
                .writeShort(2)
                .writeBoolean(false)
                .writeMediumLE(0)
                .writeMediumLE(4095)
                .writeBoolean(false)
                .writeMediumLE(5000)
                .writeMediumLE(9096);
        try {
            Assertions.assertThrows(CorruptedFrameException.class,
                    () -> new Reliability.NACK().decode(encoded));
        } finally {
            encoded.release();
        }
    }

    @Test
    public void acceptsSmallRangeAcrossSequenceWrap() {
        final ByteBuf encoded = Unpooled.buffer()
                .writeShort(1)
                .writeBoolean(false)
                .writeMediumLE(UINT.B3.MAX_VALUE - 1)
                .writeMediumLE(1);
        try {
            final Reliability.ACK ack = new Reliability.ACK();
            ack.decode(encoded);
            Assertions.assertEquals(4, ack.getEntries()[0].size());
        } finally {
            encoded.release();
        }
    }
}
