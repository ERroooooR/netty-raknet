package network.ycc.raknet.utils;

import io.netty.handler.codec.DecoderException;
import io.netty.util.internal.SystemPropertyUtil;

public class Constants {

    /**
     * Upper bound for any peer-controlled sequence gap or acknowledgement range.
     * Keeping this finite prevents a single datagram from monopolising the event loop.
     */
    public static final int MAX_PACKET_LOSS = Math.max(1, SystemPropertyUtil
            .getInt("raknetserver.maxPacketLoss", 8192));

    public static void packetLossCheck(int n, String location) {
        if (n > Constants.MAX_PACKET_LOSS) {
            throw new DecoderException("Too big packet loss at " + location + ": " + n
                    + " > " + Constants.MAX_PACKET_LOSS);
        }
    }

}
