package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.packet.Pong;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class PongHandler extends SimpleChannelInboundHandler<Pong> {

    public static final String NAME = "rn-pong";
    public static final PongHandler INSTANCE = new PongHandler();
    private static final long MAX_RTT_NANOS = 60_000_000_000L;

    protected void channelRead0(ChannelHandlerContext ctx, Pong pong) {
        if (!pong.getReliability().isReliable) {
            final long rtt = pong.getRTT();
            // Pong timestamps are supplied by the peer. Ignore forged, stale or
            // wrapped values instead of poisoning retransmission timers.
            final PingTracker tracker = ctx.channel().attr(RakNet.PING_TRACKER).get();
            if (tracker != null && tracker.acknowledge(pong.getPingTimestamp())
                    && rtt > 0 && rtt <= MAX_RTT_NANOS) {
                RakNet.config(ctx).updateRTTNanos(rtt);
                ctx.channel().attr(RakNet.LAST_INBOUND_NANOS).set(System.nanoTime());
            }
        }
    }

}
