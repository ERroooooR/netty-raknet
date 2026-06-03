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

    private static final int MAX_MISSED_PONGS = Integer.getInteger("raknetify.maxMissedPongs", 5);
    private static final long MIN_INTERVAL_MILLIS = 50;
    private static final long MAX_INTERVAL_MILLIS = 500;

    ScheduledFuture<?> pingTask = null;
    private long firstPingNanos;
    private long currentIntervalMillis = DEFAULT_INTERVAL_MILLIS;

    public void handlerAdded(ChannelHandlerContext ctx) {
        firstPingNanos = System.nanoTime();
        scheduleNextPing(ctx);
    }

    // Item 6: adaptive ping interval based on current RTT
    // interval = max(50ms, min(RTT, 500ms))
    // Fast RTT → ping more frequently → faster dead detection
    // Slow RTT → ping less frequently → avoid adding congestion
    private void scheduleNextPing(ChannelHandlerContext ctx) {
        final long rttNanos = RakNet.config(ctx).getRTTNanos();
        final long rttMillis = TimeUnit.NANOSECONDS.toMillis(rttNanos);
        currentIntervalMillis = Math.max(MIN_INTERVAL_MILLIS, Math.min(rttMillis, MAX_INTERVAL_MILLIS));
        pingTask = ctx.channel().eventLoop().schedule(() -> {
            checkDeadConnection(ctx);
            ctx.writeAndFlush(new Ping());
            scheduleNextPing(ctx);
        }, currentIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void checkDeadConnection(ChannelHandlerContext ctx) {
        final Long lastPong = ctx.channel().attr(PongHandler.LAST_PONG_NANOS).get();
        final long referenceNanos;
        final int maxMissed;
        if (lastPong == null) {
            // No pong ever received — use handlerAdded time with extra grace.
            // Floor to DEFAULT_INTERVAL_MILLIS so cold-RTT (0 samples → 50ms adaptive)
            // doesn't produce an unreasonably short initial timeout.
            referenceNanos = firstPingNanos;
            maxMissed = MAX_MISSED_PONGS * 2;
        } else {
            referenceNanos = lastPong;
            maxMissed = MAX_MISSED_PONGS;
        }
        final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - referenceNanos);
        // Always use DEFAULT_INTERVAL_MILLIS as floor so dead detection
        // is never more aggressive than 200ms * maxMissed, preventing
        // premature disconnect on low-RTT connections (min threshold: 1s).
        final long effectiveInterval = Math.max(currentIntervalMillis, DEFAULT_INTERVAL_MILLIS);
        if (elapsedMillis > effectiveInterval * maxMissed) {
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
