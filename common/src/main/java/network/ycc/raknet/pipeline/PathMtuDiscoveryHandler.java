package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.channel.ChannelFuture;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.TransportFeatures;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** RFC 8899-style positive path-MTU probing for the negotiated v12 extension. */
public final class PathMtuDiscoveryHandler extends ChannelDuplexHandler {
    public static final String NAME = "rn-plpmtud";
    private static final int PROBE_ID = 0x20;
    private static final int ACK_ID = 0x21;
    private static final long INTERVAL_SECONDS = 30;
    private ScheduledFuture<?> task;
    private ScheduledFuture<?> probeTimeout;
    private long pendingToken;
    private int pendingMtu;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        task = ctx.executor().scheduleAtFixedRate(
                () -> probe(ctx), INTERVAL_SECONDS, INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (task != null) task.cancel(false);
        if (probeTimeout != null) probeTimeout.cancel(false);
        task = null;
        probeTimeout = null;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }
        final ByteBuf in = (ByteBuf) msg;
        final int id = in.isReadable() ? in.getUnsignedByte(in.readerIndex()) : -1;
        if (id == PROBE_ID || id == ACK_ID) {
            try {
                if (in.readableBytes() < 11 || !enabled(ctx)) return;
                if (id == PROBE_ID) {
                    final long token = in.getLong(in.readerIndex() + 1);
                    final int mtu = in.getUnsignedShort(in.readerIndex() + 9);
                    if (validProbeLength(in.readableBytes(), mtu)) {
                        ctx.writeAndFlush(ctx.alloc().ioBuffer(11).writeByte(ACK_ID)
                                .writeLong(token).writeShort(mtu));
                    }
                } else if (in.readableBytes() == 11) {
                    final long token = in.getLong(in.readerIndex() + 1);
                    final int mtu = in.getUnsignedShort(in.readerIndex() + 9);
                    if (token == pendingToken && mtu == pendingMtu) {
                        final ReliabilityHandler reliability = ctx.pipeline().get(ReliabilityHandler.class);
                        if (reliability != null) reliability.adaptiveController().onProbeAck(mtu);
                        RakNet.config(ctx).getMetrics().pathMtuProbe(true, mtu);
                        RakNet.config(ctx).getMetrics().pathMtuProbeResult("acknowledged", mtu);
                        pendingToken = 0;
                        pendingMtu = 0;
                        if (probeTimeout != null) probeTimeout.cancel(false);
                        probeTimeout = null;
                    }
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
            return;
        }
        ctx.fireChannelRead(msg);
    }

    private void probe(ChannelHandlerContext ctx) {
        if (!ctx.channel().isOpen() || !enabled(ctx) || pendingToken != 0) return;
        final ReliabilityHandler reliability = ctx.pipeline().get(ReliabilityHandler.class);
        if (reliability == null) return;
        final int candidate = reliability.adaptiveController().probeCandidate();
        if (candidate < 0) return;
        pendingToken = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        pendingMtu = candidate;
        final ByteBuf probe = ctx.alloc().ioBuffer(candidate, candidate);
        probe.writeByte(PROBE_ID).writeLong(pendingToken).writeShort(candidate);
        probe.writeZero(candidate - probe.readableBytes());
        reliability.adaptiveController().onProbeSent(candidate);
        final ChannelFuture write = ctx.writeAndFlush(probe);
        write.addListener(future -> {
            if (!future.isSuccess()) {
                if (RakNet.isMessageTooLong(future.cause())) {
                    reliability.adaptiveController().onLocalMessageTooLong(candidate);
                } else {
                    ctx.fireExceptionCaught(future.cause());
                    ctx.close();
                }
                pendingToken = 0;
                pendingMtu = 0;
                if (probeTimeout != null) probeTimeout.cancel(false);
                probeTimeout = null;
            }
        });
        RakNet.config(ctx).getMetrics().pathMtuProbe(false, candidate);
        RakNet.config(ctx).getMetrics().pathMtuProbeResult("sent", candidate);
        probeTimeout = ctx.executor().schedule(() -> {
            if (pendingMtu == candidate) {
                reliability.adaptiveController().onProbeTimeout(candidate);
                pendingToken = 0;
                pendingMtu = 0;
                if (reliability.adaptiveController().shouldRetryProbe()) {
                    ctx.executor().schedule(() -> probe(ctx), 100, TimeUnit.MILLISECONDS);
                }
            }
            probeTimeout = null;
        }, 5, TimeUnit.SECONDS);
    }

    private static boolean enabled(ChannelHandlerContext ctx) {
        final Long features = ctx.channel().attr(RakNet.TRANSPORT_FEATURES).get();
        return RakNet.config(ctx).isAdaptiveTransportEnabled()
                && features != null && (features & TransportFeatures.PLPMTUD) != 0;
    }

    static boolean validProbeLength(int readableBytes, int advertisedMtu) {
        return advertisedMtu >= 11 && advertisedMtu == readableBytes;
    }
}
