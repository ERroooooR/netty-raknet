package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.packet.Ping;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

public class PingProducer implements ChannelHandler {

    public static final String NAME = "rn-ping-producer";
    public static final long DEFAULT_INTERVAL_MILLIS = Math.max(50L, Long.getLong("raknetify.pingIntervalMillis", 200L));
    private static final long IDLE_TIMEOUT_NANOS = TimeUnit.NANOSECONDS.convert(
            Math.max(30L, Long.getLong("raknetify.idleTimeoutSeconds", 60L)), TimeUnit.SECONDS);
    private static final int MAX_MISSED_PROBES = Math.max(3, Integer.getInteger("raknetify.maxMissedProbes", 5));

    ScheduledFuture<?> pingTask = null;
    private PingTracker tracker;
    private int missedProbes;

    public void handlerAdded(ChannelHandlerContext ctx) {
        tracker = new PingTracker();
        ctx.channel().attr(RakNet.PING_TRACKER).set(tracker);
        ctx.channel().attr(RakNet.LAST_INBOUND_NANOS).set(System.nanoTime());
        pingTask = ctx.channel().eventLoop().scheduleAtFixedRate(() -> {
            if (ctx.channel().isOpen()) {
                final long now = System.nanoTime();
                final Long lastInbound = ctx.channel().attr(RakNet.LAST_INBOUND_NANOS).get();
                if (lastInbound != null && now - lastInbound >= IDLE_TIMEOUT_NANOS) {
                    if (++missedProbes >= MAX_MISSED_PROBES) {
                        ctx.close();
                        return;
                    }
                } else {
                    missedProbes = 0;
                }
                final Ping ping = new Ping();
                tracker.issued(ping.getTimestamp());
                ctx.writeAndFlush(ping);
            }
        }, DEFAULT_INTERVAL_MILLIS, DEFAULT_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (pingTask != null) {
            pingTask.cancel(false);
            pingTask = null;
        }
        if (tracker != null) tracker.clear();
        ctx.channel().attr(RakNet.PING_TRACKER).set(null);
    }

    @SuppressWarnings("deprecation")
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.fireExceptionCaught(cause);
    }

}
