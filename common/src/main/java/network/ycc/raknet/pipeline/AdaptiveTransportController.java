package network.ycc.raknet.pipeline;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import network.ycc.raknet.RakNet;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/** Event-loop confined transport controller for pacing, PLPMTUD and loss classification. */
final class AdaptiveTransportController {
    enum LossType { NONE, RANDOM, BURST, MTU_BLACK_HOLE, QUEUE }

    private static final int MIN_MTU = 576;
    private static final int MTU_STEP = 32;
    private static final long DSCP_COOLDOWN = TimeUnit.SECONDS.toNanos(30);
    private static final AtomicLong LAST_DSCP_CHANGE = new AtomicLong();
    private static final AtomicInteger CURRENT_TOS = new AtomicInteger(-1);
    private static final LongAdder HEALTHY_VOTES = new LongAdder();
    private static final LongAdder CONGESTED_VOTES = new LongAdder();
    private static final int WINDOW_BUCKETS = 10;

    private final RakNet.Config config;
    private final int mtuCeiling;
    private long nextSendNanos;
    private final long[] acked = new long[WINDOW_BUCKETS];
    private final long[] lost = new long[WINDOW_BUCKETS];
    private final long[] ackedBytes = new long[WINDOW_BUCKETS];
    private int bucket;
    private long bucketStarted = System.nanoTime();
    private int consecutiveLosses;
    private int largeLosses;
    private int cleanAcks;
    private LossType lossType = LossType.NONE;
    private double packetsPerSecond = 500.0;
    private int pendingMtu;
    private long minRtt = Long.MAX_VALUE;
    private long smoothedRtt;
    private long lastDscpVote;
    private final long createdAt = System.nanoTime();
    private boolean emergencySendUsed;

    AdaptiveTransportController(RakNet.Config config) {
        this.config = config;
        this.mtuCeiling = config.getMTU();
        this.pendingMtu = config.getMTU();
    }

    int sendBudget(long nowNanos) {
        if (!config.isAdaptiveTransportEnabled()) return Integer.MAX_VALUE;
        // Permit one immediate progress datagram per pacing interval, but do not
        // allow repeated writeAndFlush calls to bypass the limiter.
        if (nowNanos < nextSendNanos) {
            if (emergencySendUsed) return 0;
            emergencySendUsed = true;
            return 1;
        }
        emergencySendUsed = false;
        final int budget = lossType == LossType.BURST || lossType == LossType.QUEUE ? 1 : 4;
        nextSendNanos = nowNanos + Math.max(200_000L, (long) (1_000_000_000D / packetsPerSecond));
        return budget;
    }

    long nanosUntilSend(long nowNanos) {
        return Math.max(0, nextSendNanos - nowNanos);
    }

    void onAck(int bytes, long rttNanos) {
        if (!config.isAdaptiveTransportEnabled()) return;
        rotate();
        acked[bucket]++;
        ackedBytes[bucket] += bytes;
        if (rttNanos > 0) {
            minRtt = Math.min(minRtt, rttNanos);
            smoothedRtt = smoothedRtt == 0 ? rttNanos : (smoothedRtt * 7 + rttNanos) / 8;
        }
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
        updatePacingRate();
    }

    void onLoss(int bytes, boolean timeout) {
        if (!config.isAdaptiveTransportEnabled()) return;
        rotate();
        lost[bucket]++;
        cleanAcks = 0;
        consecutiveLosses++;
        if (bytes >= config.getMTU() - 64 && ++largeLosses >= 3) {
            lossType = LossType.MTU_BLACK_HOLE;
            pendingMtu = Math.max(MIN_MTU, pendingMtu - MTU_STEP);
            largeLosses = 0;
        } else if (consecutiveLosses >= 3) {
            lossType = LossType.BURST;
        } else if (timeout && (lossRatio() > 0.05 || queueInflated())) {
            lossType = LossType.QUEUE;
        } else {
            lossType = LossType.RANDOM;
        }
        packetsPerSecond = Math.max(50D, packetsPerSecond * 0.75D);
        publishMetrics();
    }

    LossType lossType() { return lossType; }

    boolean shouldUseFec() {
        if (!config.isAdaptiveTransportEnabled()) return false;
        final double ratio = lossRatio();
        return lossType == LossType.RANDOM && ratio >= 0.01D && ratio <= 0.12D;
    }

    int probeCandidate() {
        return pendingMtu < mtuCeiling ? Math.min(mtuCeiling, pendingMtu + MTU_STEP) : -1;
    }

    void onProbeAck(int mtu) {
        if (mtu > pendingMtu && mtu <= mtuCeiling) pendingMtu = mtu;
    }

    void applyPendingMtu() {
        if (config.getMTU() != pendingMtu) {
            config.setMTU(pendingMtu);
            config.getMetrics().adaptiveMTU(pendingMtu);
        }
    }

    void applyDscp(Channel channel) {
        if (!config.isAdaptiveDscpEnabled() || channel == null) return;
        final long now = System.nanoTime();
        if (now - lastDscpVote >= TimeUnit.SECONDS.toNanos(1)) {
            if (lossType == LossType.BURST || lossType == LossType.QUEUE) CONGESTED_VOTES.increment();
            else HEALTHY_VOTES.increment();
            lastDscpVote = now;
        }
        final long previous = LAST_DSCP_CHANGE.get();
        if (now - previous < DSCP_COOLDOWN) return;
        final long healthy = HEALTHY_VOTES.sumThenReset();
        final long congested = CONGESTED_VOTES.sumThenReset();
        if (healthy + congested < 16) return;
        // Require a 2:1 majority to avoid players fighting over the shared socket.
        final int tos = congested > healthy * 2 ? 0x00 : healthy > congested * 2 ? 0x88 : CURRENT_TOS.get();
        if (tos >= 0 && CURRENT_TOS.getAndSet(tos) != tos
                && LAST_DSCP_CHANGE.compareAndSet(previous, now)) {
            channel.config().setOption(ChannelOption.IP_TOS, tos);
        }
    }

    private double lossRatio() {
        rotate();
        long a = 0, l = 0;
        for (int i = 0; i < WINDOW_BUCKETS; i++) { a += acked[i]; l += lost[i]; }
        return a + l == 0 ? 0D : (double) l / (a + l);
    }

    private void rotate() {
        final long now = System.nanoTime();
        long elapsed = (now - bucketStarted) / TimeUnit.SECONDS.toNanos(1);
        if (elapsed >= WINDOW_BUCKETS) {
            java.util.Arrays.fill(acked, 0);
            java.util.Arrays.fill(lost, 0);
            java.util.Arrays.fill(ackedBytes, 0);
            bucket = 0;
            bucketStarted = now;
            return;
        }
        while (elapsed-- > 0) {
            bucket = (bucket + 1) % WINDOW_BUCKETS;
            acked[bucket] = lost[bucket] = ackedBytes[bucket] = 0;
            bucketStarted += TimeUnit.SECONDS.toNanos(1);
        }
    }

    private void updatePacingRate() {
        long packets = 0;
        for (long value : acked) packets += value;
        if (packets > 0) {
            final double seconds = Math.max(1D, Math.min(10D,
                    (System.nanoTime() - createdAt) / 1_000_000_000D));
            final double target = Math.max(50D, Math.min(2000D, packets / seconds * 1.25D));
            packetsPerSecond = packetsPerSecond * 0.8D + target * 0.2D;
        }
        publishMetrics();
    }

    private boolean queueInflated() {
        return minRtt != Long.MAX_VALUE && smoothedRtt > minRtt * 2;
    }

    private void publishMetrics() {
        config.getMetrics().adaptivePacingRate(packetsPerSecond);
        config.getMetrics().adaptiveLossType(lossType.name());
    }
}
