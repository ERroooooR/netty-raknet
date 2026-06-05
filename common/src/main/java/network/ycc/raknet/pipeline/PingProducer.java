package network.ycc.raknet.pipeline;

import network.ycc.raknet.packet.Ping;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

public class PingProducer implements ChannelHandler {

    public static final String NAME = "rn-ping-producer";
    public static final long DEFAULT_INTERVAL_MILLIS = Math.max(50L, Long.getLong("raknetify.pingIntervalMillis", 200L));

    ScheduledFuture<?> pingTask = null;

    public void handlerAdded(ChannelHandlerContext ctx) {
        pingTask = ctx.channel().eventLoop().scheduleAtFixedRate(() -> {
            if (ctx.channel().isOpen()) {
                ctx.writeAndFlush(new Ping());
            }
        }, 0, DEFAULT_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (pingTask != null) {
            pingTask.cancel(false);
            pingTask = null;
        }
    }

    @SuppressWarnings("deprecation")
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.fireExceptionCaught(cause);
    }

}
