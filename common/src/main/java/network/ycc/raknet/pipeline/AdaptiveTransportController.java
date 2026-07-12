package network.ycc.raknet.pipeline;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import network.ycc.raknet.RakNet;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Event-loop confined transport controller for pacing, PLPMTUD and loss classification. */
final class AdaptiveTransportController {
    enum LossType { NONE, RANDOM, BURST, MTU_BLACK_HOLE, QUEUE }

    private static final int MIN_MTU = 576;
    private static final int MTU_STEP = 32;
    private static final long DSCP_COOLDOWN = TimeUnit.SECONDS.toNanos(30);
    private static final AtomicLong LAST_DSCP_CHANGE = new AtomicLong();

    private final RakNet.Config config;
    private final int mtuCeiling;
    private long nextSendNanos;
    private long ackedPackets;
    private long lostPackets;
    private int consecutiveLosses;
    private int largeLosses;
    private int cleanAcks;
    private LossType lossType = LossType.NONE;
    private double packetsPerSecond = 500.0;
    private int pendingMtu;

    AdaptiveTransportController(RakNet.Config config) {
        this.config = config;
        this.mtuCeiling = config.getMTU();
        this.pendingMtu = config.getMTU();
    }

    int sendBudget(long nowNanos) {
        if (nowNanos < nextSendNanos) return 0;
        final int budget = lossType == LossType.BURST || lossType == LossType.QUEUE ? 1 : 4;
        nextSendNanos = nowNanos + Math.max(200_000L, (long) (1_000_000_000D / packetsPerSecond));
        return budget;
    }

    long nanosUntilSend(long nowNanos) {
        return Math.max(0, nextSendNanos - nowNanos);
    }

    void onAck(int bytes) {
        ackedPackets++;
        consecutiveLosses = 0;
        largeLosses = 0;
        if (++cleanAcks >= 256 && pendingMtu < mtuCeiling) {
            pendingMtu = Math.min(mtuCeiling, pendingMtu + MTU_STEP);
            cleanAcks = 0;
        }
        if (lossRatio() < 0.01) {
            lossType = LossType.NONE;
            packetsPerSecond = Math.min(2000D, packetsPerSecond * 1.02D);
        }
    }

    void onLoss(int bytes, boolean timeout) {
        lostPackets++;
        cleanAcks = 0;
        consecutiveLosses++;
        if (bytes >= config.getMTU() - 64 && ++largeLosses >= 3) {
            lossType = LossType.MTU_BLACK_HOLE;
            pendingMtu = Math.max(MIN_MTU, pendingMtu - MTU_STEP);
            largeLosses = 0;
        } else if (consecutiveLosses >= 3) {
            lossType = LossType.BURST;
        } else if (timeout && lossRatio() > 0.05) {
            lossType = LossType.QUEUE;
        } else {
            lossType = LossType.RANDOM;
        }
        packetsPerSecond = Math.max(50D, packetsPerSecond * 0.75D);
    }

    LossType lossType() { return lossType; }

    void applyPendingMtu() {
        if (config.getMTU() != pendingMtu) config.setMTU(pendingMtu);
    }

    void applyDscp(Channel channel) {
        if (!Boolean.getBoolean("raknetify.adaptiveDscp") || channel == null) return;
        final long now = System.nanoTime();
        final long previous = LAST_DSCP_CHANGE.get();
        if (now - previous < DSCP_COOLDOWN || !LAST_DSCP_CHANGE.compareAndSet(previous, now)) return;
        // AF41 for healthy interactive traffic; CS0 when a provider appears to police marked traffic.
        final int tos = lossType == LossType.BURST || lossType == LossType.QUEUE ? 0x00 : 0x88;
        channel.config().setOption(ChannelOption.IP_TOS, tos);
    }

    private double lossRatio() {
        final long total = ackedPackets + lostPackets;
        return total == 0 ? 0D : (double) lostPackets / total;
    }
}
