package network.ycc.raknet;

import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.packet.Packet;
import network.ycc.raknet.pipeline.DisconnectHandler;
import network.ycc.raknet.pipeline.FrameJoiner;
import network.ycc.raknet.pipeline.FrameOrderIn;
import network.ycc.raknet.pipeline.FrameOrderOut;
import network.ycc.raknet.pipeline.FrameSplitter;
import network.ycc.raknet.pipeline.FramedPacketCodec;
import network.ycc.raknet.pipeline.PingHandler;
import network.ycc.raknet.pipeline.PongHandler;
import network.ycc.raknet.pipeline.PingTracker;
import network.ycc.raknet.pipeline.ReliabilityHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.AttributeKey;

import java.nio.channels.ClosedChannelException;

public class RakNet {

    public static final AttributeKey<Boolean> WRITABLE = AttributeKey.valueOf("RN_WRITABLE");
    public static final AttributeKey<PingTracker> PING_TRACKER = AttributeKey.valueOf("RN_PING_TRACKER");
    public static final AttributeKey<Long> LAST_INBOUND_NANOS = AttributeKey.valueOf("RN_LAST_INBOUND_NANOS");
    public static final AttributeKey<Long> TRANSPORT_FEATURES = AttributeKey.valueOf("RN_TRANSPORT_FEATURES");

    public static final ChannelOption<Long> SERVER_ID = ChannelOption.valueOf("RN_SERVER_ID");
    public static final ChannelOption<Long> CLIENT_ID = ChannelOption.valueOf("RN_CLIENT_ID");
    public static final ChannelOption<MetricsLogger> METRICS = ChannelOption.valueOf("RN_METRICS");
    public static final ChannelOption<Integer> MTU = ChannelOption.valueOf("RN_MTU");
    public static final ChannelOption<Long> RTT = ChannelOption.valueOf("RN_RTT");
    public static final ChannelOption<Integer> PROTOCOL_VERSION = ChannelOption.valueOf("RN_PROTOCOL_VERSION");
    public static final ChannelOption<Magic> MAGIC = ChannelOption.valueOf("RN_MAGIC");
    public static final ChannelOption<Long> RETRY_DELAY_NANOS = ChannelOption.valueOf("RN_RETRY_DELAY_NANOS");
    public static final ChannelOption<Integer> MAX_CONNECTIONS = ChannelOption.valueOf("RN_MAX_CONNECTIONS");
    public static final ChannelOption<Boolean> ADAPTIVE_TRANSPORT = ChannelOption.valueOf("RN_ADAPTIVE_TRANSPORT");
    public static final ChannelOption<Boolean> ADAPTIVE_DSCP = ChannelOption.valueOf("RN_ADAPTIVE_DSCP");
    public static final ChannelOption<Integer> ADAPTIVE_MIN_PPS = ChannelOption.valueOf("RN_ADAPTIVE_MIN_PPS");
    public static final ChannelOption<Integer> ADAPTIVE_MAX_PPS = ChannelOption.valueOf("RN_ADAPTIVE_MAX_PPS");
    public static final ChannelOption<Integer> SMALL_WRITE_COALESCE_MICROS = ChannelOption.valueOf("RN_SMALL_WRITE_COALESCE_MICROS");
    public static final ChannelOption<Integer> PLPMTUD_MAX_MTU = ChannelOption.valueOf("RN_PLPMTUD_MAX_MTU");

    public static final ChannelFutureListener INTERNAL_WRITE_LISTENER = future -> {
        if (!future.isSuccess() && !(future.cause() instanceof ClosedChannelException)) {
            if (isMessageTooLong(future.cause()) && future.channel().config() instanceof Config) {
                final Config config = (Config) future.channel().config();
                final int reducedMtu = Math.max(576, config.getMTU() - 32);
                future.channel().pipeline().fireUserEventTriggered(
                        TransportFeedbackEvent.packetTooBig(reducedMtu));
                return;
            }
            future.channel().pipeline().fireExceptionCaught(future.cause());
            future.channel().close();
        }
    };

    public static boolean isMessageTooLong(Throwable cause) {
        for (Throwable current = cause; current != null; current = current.getCause()) {
            final String message = current.getMessage();
            if (message != null) {
                final String lower = message.toLowerCase(java.util.Locale.ROOT);
                if (lower.contains("message too long") || lower.contains("datagram too large")
                        || lower.contains("emsgsize")) return true;
            }
        }
        return false;
    }

    public static Config config(ChannelHandlerContext ctx) {
        return config(ctx.channel());
    }

    public static Config config(Channel channel) {
        return (Config) channel.config();
    }

    public static MetricsLogger metrics(ChannelHandlerContext ctx) {
        return config(ctx).getMetrics();
    }

    /**
     * Channel specific metrics logging interface.
     */
    public interface MetricsLogger {
        default void packetsIn(int delta) {}
        default void framesIn(int delta) {}
        default void frameError(int delta) {}
        default void bytesIn(int delta) {}
        default void packetsOut(int delta) {}
        default void framesOut(int delta) {}
        default void bytesOut(int delta) {}
        default void bytesRecalled(int delta) {}
        default void bytesACKd(int delta) {}
        default void bytesNACKd(int delta) {}
        default void acksSent(int delta) {}
        default void nacksSent(int delta) {}
        default void measureRTTns(long n) {}
        default void measureRTTnsStdDev(long n) {}
        default void measureBurstTokens(int n) {}
        default void adaptivePacingRate(double packetsPerSecond) {}
        default void adaptiveDeliveryRate(long bytesPerSecond) {}
        default void adaptiveLoss(double ratio, long acknowledged, long lost) {}
        default void adaptiveLossType(String type) {}
        default void adaptiveMTU(int mtu) {}
        default void fecRecovered(int delta) {}
        default void fecParity(int packets, int bytes) {}
        default void fecExpired(int delta) {}
        default void fecBudget(int dataShards, int parityShards, double recoveryRatio) {}
        default void pathMtuProbe(boolean acknowledged, int mtu) {}
        default void pathMtuProbeResult(String result, int mtu) {}
        default void adaptiveDscp(int ipTos) {}
        default void smallWriteBatch(int frames, long delayNanos) {}
        default void pacingDelay(long delayNanos) {}
        default void congestionControl(String mode, long congestionWindowBytes, long inFlightBytes,
                                       long bandwidthBytesPerSecond, long ackAggregationBytes,
                                       double ecnCeRatio) {}
        default void pathMtuState(String state, int confirmedMtu, int probeMtu, int maximumMtu) {}

        default void currentQueuedBytes(int bytes) {}
    }

    public interface Config extends ChannelConfig {
        MetricsLogger getMetrics();

        void setMetrics(MetricsLogger metrics);

        /**
         * @return Server ID used during handshake.
         */
        long getServerId();
        void setServerId(long serverId);

        /**
         * @return Client ID used during handshake.
         */
        long getClientId();
        void setClientId(long clientId);

        /**
         * @return MTU in bytes, negotiated during handshake.
         */
        int getMTU();
        void setMTU(int mtu);

        /**
         * @return Offset used while calculating retry period.
         */
        long getRetryDelayNanos();
        void setRetryDelayNanos(long retryDelayNanos);

        long getRTTNanos();
        void setRTTNanos(long rtt);
        long getRTTStdDevNanos();
        long getMinRTTNanos();
        void updateRTTNanos(long rttSample);

        int getMaxPendingFrameSets();
        void setMaxPendingFrameSets(int maxPendingFrameSets);

        int getDefaultPendingFrameSets();
        void setDefaultPendingFrameSets(int defaultPendingFrameSets);

        int getMaxQueuedBytes();
        void setMaxQueuedBytes(int maxQueuedBytes);

        Magic getMagic();
        void setMagic(Magic magic);

        Codec getCodec();
        void setCodec(Codec codec);

        int[] getProtocolVersions();
        void setprotocolVersions(int[] protocolVersions);
        boolean containsProtocolVersion(int protocolVersion);
        int getProtocolVersion();
        void setProtocolVersion(int protocolVersion);

        int getMaxConnections();
        void setMaxConnections(int maxConnections);

        boolean isIgnoreResendGauge();
        void setIgnoreResendGauge(boolean value);

        boolean isNACKEnabled();
        void setNACKEnabled(boolean value);

        boolean isNoDelayEnabled();
        void setNoDelayEnabled(boolean value);

        boolean isAdaptiveTransportEnabled();
        void setAdaptiveTransportEnabled(boolean value);
        boolean isAdaptiveDscpEnabled();
        void setAdaptiveDscpEnabled(boolean value);
        default int getAdaptiveMinPps() { return 50; }
        default void setAdaptiveMinPps(int value) {}
        default int getAdaptiveMaxPps() { return 2000; }
        default void setAdaptiveMaxPps(int value) {}
        default int getSmallWriteCoalesceMicros() { return 250; }
        default void setSmallWriteCoalesceMicros(int value) {}
        default int getPlpmtudMaxMtu() { return 1500; }
        default void setPlpmtudMaxMtu(int value) {}
    }

    public interface Codec {
        FrameData encode(FramedPacket packet, ByteBufAllocator alloc);
        void encode(Packet packet, ByteBuf out);
        ByteBuf produceEncoded(Packet packet, ByteBufAllocator alloc);
        Packet decode(ByteBuf in);
        FramedPacket decode(FrameData data);
        int packetIdFor(Class<? extends Packet> type);
    }

    public interface Magic {
        void write(ByteBuf buf);
        void read(ByteBuf buf);
        void verify(Magic other);

        class MagicMismatchException extends CorruptedFrameException {
            public static final long serialVersionUID = 590681756L;

            public MagicMismatchException() {
                super("Incorrect RakNet magic value");
            }

            @Override
            public synchronized Throwable fillInStackTrace() {
                return this;
            }
        }
    }

    public static class ReliableFrameHandling extends ChannelInitializer<Channel> {
        public static final ReliableFrameHandling INSTANCE = new ReliableFrameHandling();

        protected void initChannel(Channel channel) {
            channel.pipeline()
                    .addLast(ReliabilityHandler.NAME,   new ReliabilityHandler())
                    .addLast(FrameJoiner.NAME,          new FrameJoiner())
                    .addLast(FrameSplitter.NAME,        new FrameSplitter())
                    .addLast(FrameOrderIn.NAME,         new FrameOrderIn())
                    .addLast(FrameOrderOut.NAME,        new FrameOrderOut())
                    .addLast(FramedPacketCodec.NAME,    new FramedPacketCodec());
        }
    }

    public static class PacketHandling extends ChannelInitializer<Channel> {
        public static final PacketHandling INSTANCE = new PacketHandling();

        protected void initChannel(Channel channel) {
            channel.pipeline()
                    .addLast(DisconnectHandler.NAME,    DisconnectHandler.INSTANCE)
                    .addLast(PingHandler.NAME,          PingHandler.INSTANCE)
                    .addLast(PongHandler.NAME,          PongHandler.INSTANCE);
        }
    }

}
