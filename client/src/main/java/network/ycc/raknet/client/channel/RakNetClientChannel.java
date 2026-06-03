package network.ycc.raknet.client.channel;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.channel.DatagramChannelProxy;
import network.ycc.raknet.client.RakNetClient;
import network.ycc.raknet.client.pipeline.ConnectionInitializer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;
import io.netty.util.concurrent.ScheduledFuture;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class RakNetClientChannel extends DatagramChannelProxy {
    private static final byte GATE_ROUTE_HINT_PACKET_ID = (byte) 0xFE;
    private static final byte GATE_ROUTE_HINT_VERSION = 2;
    private static final int GATE_ROUTE_HINT_TOKEN_LENGTH = 16;
    private static final int GATE_ROUTE_HINT_MAX_HOST_LENGTH = 1024;
    private static final int GATE_ROUTE_HINT_RETRIES = 4;
    private static final int GATE_ROUTE_HINT_RETRY_DELAY_MILLIS = 50;
    private static final int GATE_ROUTE_HINT_REFRESH_MILLIS = 5000;
    private static final byte[] GATE_ROUTE_HINT_MAGIC = "GATE_RAKNET_ROUTE".getBytes(StandardCharsets.US_ASCII);

    protected final ChannelPromise connectPromise;
    protected final byte[] gateRouteHintToken = new byte[GATE_ROUTE_HINT_TOKEN_LENGTH];
    protected volatile String gateRouteHint;
    protected volatile ScheduledFuture<?> gateRouteHintRefreshTask;

    public RakNetClientChannel() {
        this(NioDatagramChannel.class);
    }

    public RakNetClientChannel(Supplier<? extends DatagramChannel> ioChannelSupplier) {
        super(ioChannelSupplier);
        ThreadLocalRandom.current().nextBytes(gateRouteHintToken);
        connectPromise = newPromise();
        addDefaultPipeline();
    }

    public RakNetClientChannel(Class<? extends DatagramChannel> ioChannelType) {
        super(ioChannelType);
        ThreadLocalRandom.current().nextBytes(gateRouteHintToken);
        connectPromise = newPromise();
        addDefaultPipeline();
    }

    public ChannelFuture connectFuture() {
        return connectPromise;
    }

    public void setGateRouteHint(String gateRouteHint) {
        this.gateRouteHint = gateRouteHint;
    }

    @Override
    public boolean isActive() {
        return super.isActive() && connectPromise.isSuccess();
    }

    @Override
    public boolean isWritable() {
        final Boolean result = attr(RakNet.WRITABLE).get();
        return (result == null || result) && super.isWritable();
    }

    protected void addDefaultPipeline() {
        pipeline()
                .addLast(newClientHandler())
                .addLast(RakNetClient.DefaultClientInitializer.INSTANCE);
        connectPromise.addListener(res -> {
            if (!res.isSuccess()) {
                RakNetClientChannel.this.close();
            }
        });
        closeFuture().addListener(res -> cancelGateRouteHintRefresh());
    }

    protected ChannelHandler newClientHandler() {
        return new ClientHandler();
    }

    protected void cancelGateRouteHintRefresh() {
        final ScheduledFuture<?> task = gateRouteHintRefreshTask;
        if (task != null) {
            gateRouteHintRefreshTask = null;
            task.cancel(false);
        }
    }

    protected class ClientHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                SocketAddress localAddress, ChannelPromise promise) {
            try {
                if (!(remoteAddress instanceof InetSocketAddress)) {
                    throw new IllegalArgumentException(
                            "Provided remote address is not an InetSocketAddress");
                }
                if (listener.isActive()) {
                    throw new IllegalStateException("Channel connection already started");
                }
                final ChannelFuture listenerConnect = listener.connect(remoteAddress, localAddress);
                listenerConnect.addListener(udpConnectResult -> {
                    if (udpConnectResult.isSuccess()) {
                        final ChannelFuture routeHintWrite = writeGateRouteHintIfNeeded();
                        routeHintWrite.addListener(routeHintResult -> {
                            if (routeHintResult.isSuccess()) {
                                //start connection process
                                pipeline().replace(ConnectionInitializer.NAME, ConnectionInitializer.NAME,
                                        new ConnectionInitializer(connectPromise));
                            } else {
                                connectPromise.tryFailure(routeHintResult.cause());
                            }
                        });
                    }
                });
                final PromiseCombiner combiner = new PromiseCombiner();
                combiner.add(listenerConnect);
                combiner.add((ChannelFuture) connectPromise);
                combiner.finish(promise);
            } catch (Exception t) {
                promise.tryFailure(t);
            }
        }

        private ChannelFuture writeGateRouteHintIfNeeded() {
            final String host = gateRouteHint;
            if (host == null || host.isEmpty()) {
                return newSucceededFuture();
            }
            final byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);
            if (hostBytes.length == 0 || hostBytes.length > GATE_ROUTE_HINT_MAX_HOST_LENGTH) {
                return newFailedFuture(new IllegalArgumentException("Invalid Gate RakNet route hint host length"));
            }

            final ChannelFuture firstWrite = writeGateRouteHint(hostBytes);
            scheduleGateRouteHintRefresh(hostBytes);
            for (int i = 1; i < GATE_ROUTE_HINT_RETRIES; i++) {
                final int delayMillis = GATE_ROUTE_HINT_RETRY_DELAY_MILLIS * i;
                listener.eventLoop().schedule(() -> {
                    if (connectPromise.isDone() || !listener.isOpen()) {
                        return;
                    }
                    writeGateRouteHint(hostBytes).addListener(RakNet.INTERNAL_WRITE_LISTENER);
                }, delayMillis, TimeUnit.MILLISECONDS);
            }
            return firstWrite;
        }

        private void scheduleGateRouteHintRefresh(byte[] hostBytes) {
            if (gateRouteHintRefreshTask != null) {
                return;
            }
            gateRouteHintRefreshTask = listener.eventLoop().scheduleAtFixedRate(() -> {
                if (!listener.isOpen() || !RakNetClientChannel.this.isOpen()) {
                    cancelGateRouteHintRefresh();
                    return;
                }
                if (connectPromise.isDone() && !connectPromise.isSuccess()) {
                    cancelGateRouteHintRefresh();
                    return;
                }
                if (connectPromise.isSuccess()) {
                    writeGateRouteHint(hostBytes).addListener(RakNet.INTERNAL_WRITE_LISTENER);
                }
            }, GATE_ROUTE_HINT_REFRESH_MILLIS, GATE_ROUTE_HINT_REFRESH_MILLIS, TimeUnit.MILLISECONDS);
        }

        private ChannelFuture writeGateRouteHint(byte[] hostBytes) {
            final ByteBuf buf = alloc().ioBuffer(1 + GATE_ROUTE_HINT_MAGIC.length + 1 + GATE_ROUTE_HINT_TOKEN_LENGTH + 2 + hostBytes.length);
            try {
                buf.writeByte(GATE_ROUTE_HINT_PACKET_ID & 0xFF);
                buf.writeBytes(GATE_ROUTE_HINT_MAGIC);
                buf.writeByte(GATE_ROUTE_HINT_VERSION);
                buf.writeBytes(gateRouteHintToken);
                buf.writeShort(hostBytes.length);
                buf.writeBytes(hostBytes);
                return listener.writeAndFlush(buf);
            } catch (Throwable t) {
                ReferenceCountUtil.release(buf);
                return newFailedFuture(t);
            }
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            listener.write(msg, wrapPromise(promise));
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof DatagramPacket) {
                try {
                    final DatagramPacket datagram = (DatagramPacket) msg;
                    if (datagram.sender() == null || datagram.sender().equals(remoteAddress())) {
                        ctx.fireChannelRead(datagram.content().retain());
                    }
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            } else {
                ctx.fireChannelRead(msg);
            }
        }

        @Override
        public void read(ChannelHandlerContext ctx) {
            // NOOP
        }
    }
}
