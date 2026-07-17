package network.ycc.raknet;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;
import network.ycc.raknet.channel.DatagramChannelProxy;
import network.ycc.raknet.client.channel.RakNetClientChannel;
import network.ycc.raknet.client.channel.RakNetClientThreadedChannel;
import network.ycc.raknet.config.DefaultCodec;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.packet.Ping;
import network.ycc.raknet.packet.Reliability;
import network.ycc.raknet.pipeline.UserDataCodec;
import network.ycc.raknet.pipeline.RawPacketCodec;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import network.ycc.raknet.utils.EmptyInit;
import network.ycc.raknet.utils.MockDatagram;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class EndToEndTest {
    final EventLoopGroup ioGroup = new NioEventLoopGroup();
    final EventLoopGroup childGroup = new NioEventLoopGroup();
    final InetSocketAddress mockServerAddress = new InetSocketAddress("localhost", 31745);
    final InetSocketAddress mockClientAddress = new InetSocketAddress("localhost", 31746);
    volatile InetSocketAddress serverAddress;

    @AfterEach
    public void shutdownEventLoops() throws InterruptedException {
        ioGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
        childGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }

    public static ChannelInitializer<Channel> simpleHandler(
            BiConsumer<ChannelHandlerContext, Object> func) {
        return new ChannelInitializer<Channel>() {
            protected void initChannel(Channel ch) {
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        func.accept(ctx, msg);
                    }
                });
                ch.eventLoop().schedule(() -> {
                    final ByteBuf buf1 = ch.alloc().buffer(1).writeByte(0);
                    ch.write(FrameData.create(ch.alloc(), 0xF0, buf1));
                    buf1.release();
                }, 10, TimeUnit.MILLISECONDS);
            }
        };
    }

    @Test
    public void serverCloseTest() throws Throwable {
        for (int i = 0; i < 20; i++) {
            newServer(null, null, null).close().sync();
        }
    }

    @Test
    public void connectAndCloseTestServerFirst() throws Throwable {
        for (int i = 0; i < 5; i++) {
            Channel server = newServer(null, null, null);
            Channel client = newClient(null, null);

            server.close().sync();
            client.close().sync();
        }
    }

    @Test
    public void connectAndCloseTestClientFirst() throws Throwable {
        for (int i = 0; i < 20; i++) {
            Channel server = newServer(null, null, null);
            Channel client = newClient(null, null);

            client.close().sync();
            server.close().sync();
        }
    }

    @Test
    public void singleBufferTest() throws Throwable {
        int bytesSent = 1000;
        AtomicInteger bytesRecvd = new AtomicInteger(0);

        Channel server = newServer(null, simpleHandler((ctx, msg) -> {
            if (msg instanceof ByteBuf) {
                bytesRecvd.addAndGet(((ByteBuf) msg).readableBytes());
            }
            ReferenceCountUtil.safeRelease(msg);
        }), null);
        Channel client = newClient(null, null);

        //add some bad frame data, should be ignore safely
        client.parent().pipeline().fireChannelRead(Unpooled.wrappedBuffer(
                new byte[]{(byte) DefaultCodec.FRAME_DATA_START, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

        Thread.sleep(100); // exercise a write after transport keepalive traffic consumed the initial token
        client.pipeline().write(Unpooled.wrappedBuffer(new byte[bytesSent]));
        client.pipeline().flush();

        Thread.sleep(1000); //give pings time to run

        server.close().sync();
        client.close().sync();
        System.gc();
        System.gc();

        Assertions.assertEquals(bytesSent, bytesRecvd.get());
    }

    @Test
    public void protocol12NegotiatesAdaptiveFeatures() throws Throwable {
        final Channel server = newServer(null, null, null);
        final Channel client = newClient(null, null, 12);
        Assertions.assertTrue(client instanceof RakNetClientThreadedChannel);
        Assertions.assertTrue(client.parent() instanceof RakNetClientChannel);
        Assertions.assertEquals(TransportFeatures.SUPPORTED,
                client.parent().attr(RakNet.TRANSPORT_FEATURES).get().longValue());
        client.close().sync();
        server.close().sync();
    }

    @Test
    public void protocol12FallsBackToLegacyServer() throws Throwable {
        final Channel server = newServer(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) {
                RakNet.config(ch).setprotocolVersions(new int[]{9, 10, 11});
            }
        }, null, null);
        final Channel client = newClient(null, null, 12);
        Assertions.assertEquals(11, RakNet.config(client.parent()).getProtocolVersion());
        Assertions.assertEquals(0L,
                client.parent().attr(RakNet.TRANSPORT_FEATURES).get().longValue());
        client.close().sync();
        server.close().sync();
    }

    @Test
    public void manyBufferTest() throws Throwable {
        dataTest(100, 5000, false, false, false);
    }

    @Test
    public void manyBufferBadClient() throws Throwable {
        dataTest(100, 1000, false, false, false);
    }

    @Test
    public void manyBufferBadServer() throws Throwable {
        dataTest(100, 1000, true, true, false);
    }

    @Test
    public void manyBufferBadBoth() throws Throwable {
        dataTest(100, 5000, true, true, false);
    }

    @Test
    public void adaptiveV12SurvivesModerateRandomLoss() throws Throwable {
        dataTest(200, 1000, true, true, false, 12, 0.05);
    }

    @Test
    public void adaptiveAckProtectionRecoversWhenEveryFirstAckIsLost() throws Throwable {
        final AtomicReference<Channel> serverChild = new AtomicReference<>();
        final Channel server = newServer(null, new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) {
                serverChild.set(ch);
            }
        }, null);
        final Channel client = newClient(null, null, 12);
        final RecoveryMetrics metrics = new RecoveryMetrics();
        RakNet.config(client.parent()).setMetrics(metrics);

        final Set<String> seenAcks = new CopyOnWriteArraySet<>();
        final AtomicInteger droppedFirstAcks = new AtomicInteger();
        final AtomicInteger passedRepeatedAcks = new AtomicInteger();
        client.parent().pipeline().addAfter(RawPacketCodec.NAME,
                "drop-first-ack", new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                    throws Exception {
                if (msg instanceof Reliability.ACK) {
                    final StringBuilder signatureBuilder = new StringBuilder();
                    for (Reliability.REntry entry : ((Reliability.ACK) msg).getEntries()) {
                        signatureBuilder.append(entry.idStart).append(':')
                                .append(entry.idFinish).append(',');
                    }
                    final String signature = signatureBuilder.toString();
                    if (seenAcks.add(signature)) {
                        droppedFirstAcks.incrementAndGet();
                        ReferenceCountUtil.release(msg);
                        promise.trySuccess();
                        return;
                    }
                    passedRepeatedAcks.incrementAndGet();
                }
                super.write(ctx, msg, promise);
            }
        });

        for (int i = 0; i < 100 && serverChild.get() == null; i++) {
            Thread.sleep(10);
        }
        final Channel child = serverChild.get();
        Assertions.assertNotNull(child);
        final ChannelFuture[] writes = new ChannelFuture[8];
        for (int i = 0; i < writes.length; i++) {
            final ByteBuf payload = Unpooled.buffer(8).writeLong(i);
            final FrameData packet = FrameData.create(child.alloc(), 0xFE, payload);
            payload.release();
            packet.setReliability(FramedPacket.Reliability.RELIABLE);
            writes[i] = child.pipeline().write(packet);
            child.pipeline().flush();
            Thread.sleep(30);
        }

        try {
            for (ChannelFuture write : writes) {
                write.get(10, TimeUnit.SECONDS);
                Assertions.assertTrue(write.isSuccess());
            }
            final String diagnostics = "dropped=" + droppedFirstAcks.get()
                    + ", duplicates=" + metrics.reliableDuplicates.get()
                    + ", repeatedPackets=" + metrics.ackRepeatedPackets.get()
                    + ", passedRepeats=" + passedRepeatedAcks.get();
            Assertions.assertTrue(droppedFirstAcks.get() >= 3, diagnostics);
            Assertions.assertTrue(metrics.reliableDuplicates.get() >= 3, diagnostics);
            Assertions.assertTrue(metrics.ackRepeatedPackets.get() > 0, diagnostics);
            Assertions.assertTrue(passedRepeatedAcks.get() > 0, diagnostics);
        } finally {
            client.close().sync();
            server.close().sync();
        }
    }

    @Test
    public void fireGC() {
        System.gc();
    }

    public void dataTest(int nSend, int maxSize, boolean brutalizeWrite, boolean brutalizeRead,
            boolean mockTransport) throws Throwable {
        dataTest(nSend, maxSize, brutalizeWrite, brutalizeRead, mockTransport, 11, 0.20);
    }

    public void dataTest(int nSend, int maxSize, boolean brutalizeWrite, boolean brutalizeRead,
            boolean mockTransport, int protocolVersion, double lossPercent) throws Throwable {
        Random rnd = new Random(34598);
        AtomicInteger bytesSent = new AtomicInteger(0);
        AtomicInteger bytesRecvd = new AtomicInteger(0);
        AtomicInteger numRecvd = new AtomicInteger(0);
        AtomicInteger pending = new AtomicInteger(0);
        ConcurrentHashMap<Long, Object> unreliableSet = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Object> reliableSet = new ConcurrentHashMap<>();
        EventLoop testLoop = childGroup.next();
        PromiseCombiner combiner = new PromiseCombiner();
        MockDatagramPair mockPair = mockTransport ? new MockDatagramPair() : null;
        Brutalizer brutalizer = new Brutalizer();

        Channel server = newServer(null, simpleHandler((ctx, msg) -> {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                if (buf.readableBytes() == 8) {
                    long value = buf.readLong();
                    reliableSet.remove(value);
                } else {
                    numRecvd.incrementAndGet();
                    bytesRecvd.addAndGet(buf.readableBytes());
                }
            }
            ReferenceCountUtil.safeRelease(msg);
        }), mockPair);
        Channel client = newClient(null, mockPair, protocolVersion);
        ChannelPromise donePromise = client.newPromise();

        client.parent().pipeline()
                .addAfter(DatagramChannelProxy.LISTENER_HANDLER_NAME, "brutalizer", brutalizer);
        brutalizer.rnd = rnd;
        brutalizer.brutalizeRead = brutalizeRead;
        brutalizer.brutalizeWrite = brutalizeWrite;
        brutalizer.lossPercent = lossPercent;

        //TODO: server side writes?

        for (int i = 0; i < nSend; i++) {
            int size = rnd.nextInt(maxSize) + 1;
            if (size == 8) {
                size = 9; //reserve 8 size for the other tests
            }
            while (!client.isWritable() || pending.get() > 3000) {
                Thread.yield();
                //TODO: deadline check
            }
            ChannelFuture fut;
            switch (rnd.nextInt(2)) {
                case 0:
                    fut = client.pipeline().write(Unpooled.wrappedBuffer(new byte[size]));
                    break;
                default:
                    FrameData data = FrameData
                            .create(client.alloc(), 0xFE, Unpooled.wrappedBuffer(new byte[size]));
                    if (rnd.nextBoolean()) {
                        data.setReliability(FramedPacket.Reliability.RELIABLE_ORDERED);
                        data.setOrderChannel(rnd.nextInt(4));
                    }
                    fut = client.pipeline().write(data);
            }
            testLoop.execute(() -> combiner.add(fut));
            bytesSent.addAndGet(size);
            pending.incrementAndGet();
            fut.addListener(x -> pending.decrementAndGet());
        }

        for (int i = 0; i < 200; i++) {
            long value = rnd.nextLong();
            reliableSet.put(value, true);
            ByteBuf buf = Unpooled.wrappedBuffer(new byte[8]);
            buf.clear();
            buf.writeLong(value);
            FrameData packet = FrameData.create(client.alloc(), 0xFE, buf);
            packet.setReliability(FramedPacket.Reliability.RELIABLE);
            testLoop.execute(() -> combiner.add(client.pipeline().write(packet)));
        }

        client.pipeline().flush();
        client.write(new Ping()).sync();

        testLoop.execute(() -> combiner.finish(donePromise));

        //TODO: new test loop for UNRELIABLE + RELIABLE

        try {
            donePromise.get(90, TimeUnit.SECONDS);
        } finally {
            server.close().sync();
            client.close().sync();
        }
        System.gc();
        System.gc();
        System.gc();
        Assertions.assertTrue(reliableSet.isEmpty());
        Assertions.assertEquals(0, pending.get());
        Assertions.assertEquals(nSend, numRecvd.get());
        Assertions.assertEquals(bytesSent.get(), bytesRecvd.get());
    }

    public Channel newServer(ChannelInitializer<Channel> ioInit,
            final ChannelInitializer<Channel> childInit, MockDatagramPair dgPair)
            throws InterruptedException {
        if (ioInit == null) {
            ioInit = new EmptyInit();
        }
        final ServerBootstrap bootstrap = new ServerBootstrap()
                .group(ioGroup, childGroup)
                .channelFactory(() -> new RakNetServerChannel(() -> {
                    if (dgPair != null) {
                        return dgPair.server;
                    } else {
                        return new NioDatagramChannel();
                    }
                }))
                .option(RakNet.SERVER_ID, 12345L)
                .option(RakNet.RETRY_DELAY_NANOS,
                        TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS))
                .handler(ioInit)
                .childHandler(new ChannelInitializer<Channel>() {
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(UserDataCodec.NAME, new UserDataCodec(0xFE));
                        if (childInit != null) {
                            ch.pipeline().addLast(childInit);
                        }
                    }
                });
        final InetSocketAddress bindAddress = dgPair == null
                ? new InetSocketAddress("127.0.0.1", 0) : mockServerAddress;
        final Channel channel = bootstrap.bind(bindAddress).sync().channel();
        serverAddress = (InetSocketAddress) channel.localAddress();
        return channel;
    }

    public Channel newClient(ChannelInitializer<Channel> init, MockDatagramPair dgPair)
            throws InterruptedException {
        return newClient(init, dgPair, 11);
    }

    public Channel newClient(ChannelInitializer<Channel> init, MockDatagramPair dgPair, int protocolVersion)
            throws InterruptedException {
        final Bootstrap bootstrap = new Bootstrap()
                .group(ioGroup)
                .channelFactory(() -> new RakNetClientThreadedChannel(() -> {
                    if (dgPair != null) {
                        return dgPair.client;
                    } else {
                        return new NioDatagramChannel();
                    }
                }))
                .option(RakNet.CLIENT_ID, 6789L)
                .option(RakNet.PROTOCOL_VERSION, protocolVersion)
                .option(RakNet.RETRY_DELAY_NANOS,
                        TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS))
                .handler(new ChannelInitializer<Channel>() {
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(UserDataCodec.NAME, new UserDataCodec(0xFE));
                        if (init != null) {
                            ch.pipeline().addLast(init);
                        }
                    }
                });
        final InetSocketAddress target = dgPair == null ? serverAddress : mockServerAddress;
        if (target == null) throw new IllegalStateException("server must be started before the client");
        return bootstrap.connect(target).sync().channel();
    }

    public static class Brutalizer extends ChannelDuplexHandler {
        Random rnd;
        boolean brutalizeWrite = false;
        boolean brutalizeRead = false;
        double lossPercent = 0.20;
        double dupePercent = 0.20;
        double orderPercent = 0.20;

        Object writeStash = null;
        Object readStash = null;

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            ReferenceCountUtil.safeRelease(writeStash);
            ReferenceCountUtil.safeRelease(readStash);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                throws Exception {
            if (brutalizeWrite) {
                if (rnd.nextDouble() < orderPercent && writeStash != null) {
                    ctx.write(writeStash);
                    writeStash = null;
                }
                if (rnd.nextDouble() < lossPercent) {
                    if (rnd.nextDouble() < orderPercent) {
                        ReferenceCountUtil.safeRelease(writeStash);
                        writeStash = msg;
                    } else {
                        ReferenceCountUtil.release(msg);
                    }
                    promise.trySuccess();
                    return;
                }
                if (rnd.nextDouble() < dupePercent) {
                    super.write(ctx, ReferenceCountUtil.retain(msg), ctx.voidPromise());
                }
            }
            super.write(ctx, msg, promise);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (brutalizeRead) {
                if (rnd.nextDouble() < orderPercent && readStash != null) {
                    ctx.write(readStash);
                    readStash = null;
                }
                if (rnd.nextDouble() < lossPercent) {
                    if (rnd.nextDouble() < orderPercent) {
                        ReferenceCountUtil.safeRelease(readStash);
                        readStash = msg;
                    } else {
                        ReferenceCountUtil.release(msg);
                    }
                    return;
                }
                if (rnd.nextDouble() < dupePercent) {
                    super.channelRead(ctx, ReferenceCountUtil.retain(msg));
                }
            }
            super.channelRead(ctx, msg);
        }
    }

    public class MockDatagramPair {
        final MockDatagram server = new MockDatagram(null, mockServerAddress, mockClientAddress);
        final MockDatagram client = new MockDatagram(null, mockClientAddress, mockServerAddress);

        {
            server.writeOut = dg -> client.pipeline().fireChannelRead(dg).fireChannelReadComplete();
            client.writeOut = dg -> server.pipeline().fireChannelRead(dg).fireChannelReadComplete();
        }
    }

    private static final class RecoveryMetrics implements RakNet.MetricsLogger {
        final AtomicInteger reliableDuplicates = new AtomicInteger();
        final AtomicInteger ackRepeatedPackets = new AtomicInteger();

        @Override
        public void reliableFrameDuplicate(int delta) {
            reliableDuplicates.addAndGet(delta);
        }

        @Override
        public void ackRepeated(int acknowledgedFrameSets) {
            ackRepeatedPackets.incrementAndGet();
        }
    }

}
