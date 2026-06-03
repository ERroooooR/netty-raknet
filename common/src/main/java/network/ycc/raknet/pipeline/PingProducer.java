package network.ycc.raknet.pipeline;

import network.ycc.raknet.packet.Ping;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

public class PingProducer implements ChannelHandler {

    public static final String NAME = "rn-ping-producer";
    public static final long DEFAULT_INTERVAL_MILLIS = Math.max(50L, Long.getLong("raknetify.pingIntervalMillis", 200L));

    private static final int MAX_MISSED_PONGS = Integer.getInteger("raknetify.maxMissedPongs", 5);

    ScheduledFuture<?> pingTask = null;

    public void handlerAdded(ChannelHandlerContext ctx) {
        pingTask = ctx.channel().eventLoop().scheduleAtFixedRate(() -> {
            checkDeadConnection(ctx);
            ctx.writeAndFlush(new Ping());
        }, 0, DEFAULT_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    private void checkDeadConnection(ChannelHandlerContext ctx) {
        final Long lastPong = ctx.channel().attr(PongHandler.LAST_PONG_NANOS).get();
        if (lastPong == null) return; // too early, no pong yet
        final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastPong);
        if (elapsedMillis > DEFAULT_INTERVAL_MILLIS * MAX_MISSED_PONGS) {
            final long seconds = elapsedMillis / 1000;
            System.err.println("Raknetify: no pong for " + seconds + "s, closing connection");
            ctx.close();
        }
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
