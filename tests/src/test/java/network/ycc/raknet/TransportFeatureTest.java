package network.ycc.raknet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import network.ycc.raknet.packet.ConnectionRequest;
import network.ycc.raknet.packet.ServerHandshake;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

public class TransportFeatureTest {
    @Test
    public void connectionRequestNegotiatesFeaturesWithoutChangingLegacyEncoding() {
        final ByteBuf legacy = Unpooled.buffer();
        new ConnectionRequest(42).encode(legacy);
        Assertions.assertEquals(17, legacy.readableBytes());

        final ByteBuf extended = Unpooled.buffer();
        new ConnectionRequest(42, TransportFeatures.SUPPORTED).encode(extended);
        final ConnectionRequest decoded = new ConnectionRequest();
        decoded.decode(extended);
        Assertions.assertEquals(TransportFeatures.SUPPORTED, decoded.getTransportFeatures());
        legacy.release();
        extended.release();
    }

    @Test
    public void serverHandshakeExtensionDoesNotConfuseAddressParsing() {
        final ServerHandshake original = new ServerHandshake(
                new InetSocketAddress("127.0.0.1", 19132), 1234L, 3, TransportFeatures.FEC);
        final ByteBuf encoded = Unpooled.buffer();
        original.encode(encoded);
        final ServerHandshake decoded = new ServerHandshake();
        decoded.decode(encoded);
        Assertions.assertEquals(3, decoded.getnExtraAddresses());
        Assertions.assertEquals(TransportFeatures.FEC, decoded.getTransportFeatures());
        Assertions.assertEquals(1234L, decoded.getTimestamp());
        encoded.release();
    }
}
