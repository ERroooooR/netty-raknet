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
    private static final long PROBE_RETRY_NANOS = TimeUnit.MINUTES.toNanos(5);

    private final RakNet.Config config;
    private final int mtuCeiling;
    private long nextSendNanos;
    private long tokenUpdatedNanos;
    private double pacingTokens = 1D;
    private final long[] acked = new long[WINDOW_BUCKETS];
    private final long[] lost = new long[WINDOW_BUCKETS];
    private final long[] ackedBytes = new long[WINDOW_BUCKETS];
    private int bucket;
    private long bucketStarted = System.nanoTime();
    private int consecutiveLosses;
    private int largeLosses;
    private long lastLargeLossNanos;
    private int cleanAcks;
    private LossType lossType = LossType.NONE;
    private double packetsPerSecond;
    private int pendingMtu;
    private long minRtt = Long.MAX_VALUE;
    private long smoothedRtt;
    private long deliveryRateBytesPerSecond;
    private long lastAckNanos;
    private long lastDscpVote;
    private final long createdAt = System.nanoTime();
    private int probeUpperMtu;
    private long lastProbeFailure;

    AdaptiveTransportController(RakNet.Config config) {
        this.config = config;
        this.mtuCeiling = config.getMTU();
        this.pendingMtu = config.getMTU();
        this.probeUpperMtu = mtuCeiling;
        this.packetsPerSecond = clampPps(500D);
        config.getMetrics().adaptiveMTU(pendingMtu);
    }

    int sendBudget(long nowNanos) {
        if (!config.isAdaptiveTransportEnabled()) return Integer.MAX_VALUE;
        final int burst = lossType == LossType.BURST || lossType == LossType.QUEUE ? 1 : 4;
        if (tokenUpdatedNanos == 0) {
            tokenUpdatedNanos = nowNanos;
        } else if (nowNanos > tokenUpdatedNanos) {
            pacingTokens = Math.min(burst, pacingTokens
                    + (nowNanos - tokenUpdatedNanos) * packetsPerSecond / 1_000_000_000D);
            tokenUpdatedNanos = nowNanos;
        }
        pacingTokens = Math.min(burst, pacingTokens);
        int budget = Math.max(0, (int) pacingTokens);
        if (budget > 0) {
            pacingTokens -= budget;
        } else if (pacingTokens >= 0D) {
            // One progress datagram may borrow the next token. The debt blocks
            // repeated flushes until it is repaid, so explicit flush remains
            // reliable without raising the long-term PPS rate.
            budget = 1;
            pacingTokens -= 1D;
        }
        final double missing = Math.max(0D, -pacingTokens);
        nextSendNanos = nowNanos + Math.max(10_000L,
                (long) Math.ceil(missing * 1_000_000_000D / packetsPerSecond));
        return budget;
    }

    long nanosUntilSend(long nowNanos) {
        final long delay = Math.max(0, nextSendNanos - nowNanos);
        config.getMetrics().pacingDelay(delay);
        return delay;
    }

    void onAck(int bytes, long rttNanos) {
        if (!config.isAdaptiveTransportEnabled()) return;
        rotate();
        acked[bucket]++;
        ackedBytes[bucket] += bytes;
        final long now = System.nanoTime();
        if (lastAckNanos != 0 && now > lastAckNanos) {
            final long sample = Math.min(Long.MAX_VALUE / TimeUnit.SECONDS.toNanos(1), bytes)
                    * TimeUnit.SECONDS.toNanos(1) / (now - lastAckNanos);
            deliveryRateBytesPerSecond = deliveryRateBytesPerSecond == 0 ? sample
                    : (deliveryRateBytesPerSecond * 7 + sample) / 8;
        }
        lastAckNanos = now;
        if (rttNanos > 0) {
            minRtt = Math.min(minRtt, rttNanos);
            smoothedRtt = smoothedRtt == 0 ? rttNanos : (smoothedRtt * 7 + rttNanos) / 8;
        }
        consecutiveLosses = 0;
        if (bytes >= config.getMTU() - 64) {
            largeLosses = 0;
            lastLargeLossNanos = 0;
        }
        if (++cleanAcks >= 256 && pendingMtu < mtuCeiling) {
            pendingMtu = Math.min(mtuCeiling, pendingMtu + MTU_STEP);
            cleanAcks = 0;
        }
        if (lossRatio() < 0.01) {
            lossType = LossType.NONE;
            packetsPerSecond = clampPps(packetsPerSecond * 1.02D);
        }
        updatePacingRate();
    }

    void onLoss(int bytes, boolean timeout) {
        if (!config.isAdaptiveTransportEnabled()) return;
        rotate();
        lost[bucket]++;
        cleanAcks = 0;
        consecutiveLosses++;
        final long now = System.nanoTime();
        if (bytes >= config.getMTU() - 64
                && recordLargeLoss(now) >= 3) {
            lossType = LossType.MTU_BLACK_HOLE;
            pendingMtu = Math.max(MIN_MTU, pendingMtu - MTU_STEP);
            largeLosses = 0;
        } else if (timeout && (lossRatio() > 0.05 || queueInflated())) {
            lossType = LossType.QUEUE;
        } else if (consecutiveLosses >= 3) {
            lossType = LossType.BURST;
        } else {
            lossType = LossType.RANDOM;
        }
        packetsPerSecond = clampPps(packetsPerSecond * 0.75D);
        publishMetrics();
    }

    LossType lossType() { return lossType; }

    boolean shouldUseFec() {
        if (!config.isAdaptiveTransportEnabled()) return false;
        final double ratio = lossRatio();
        return lossType == LossType.RANDOM && ratio >= 0.01D && ratio <= 0.12D;
    }

    int probeCandidate() {
        final long now = System.nanoTime();
        if (probeUpperMtu <= pendingMtu && now - lastProbeFailure >= PROBE_RETRY_NANOS) {
            probeUpperMtu = mtuCeiling;
        }
        if (pendingMtu >= probeUpperMtu) return -1;
        final int gap = probeUpperMtu - pendingMtu;
        if (gap <= MTU_STEP) return probeUpperMtu;
        // Converge quickly after a downshift but keep candidates aligned for
        // reproducible packet captures and common tunnel MTUs.
        final int step = Math.max(MTU_STEP, (gap / 2) & ~7);
        return Math.min(probeUpperMtu, pendingMtu + step);
    }

    void onProbeAck(int mtu) {
        if (mtu > pendingMtu && mtu <= mtuCeiling) {
            pendingMtu = mtu;
            probeUpperMtu = mtuCeiling;
            lastProbeFailure = 0;
        }
    }

    void onProbeTimeout(int mtu) {
        if (mtu > pendingMtu && mtu <= probeUpperMtu) {
            probeUpperMtu = Math.max(pendingMtu, mtu - 1);
            lastProbeFailure = System.nanoTime();
        }
        config.getMetrics().pathMtuProbeResult("timeout", mtu);
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
        if (tos >= 0 && CURRENT_TOS.get() != tos
                && LAST_DSCP_CHANGE.compareAndSet(previous, now)) {
            if (channel.config().setOption(ChannelOption.IP_TOS, tos)) {
                CURRENT_TOS.set(tos);
                config.getMetrics().adaptiveDscp(tos);
            }
        }
    }

    private double lossRatio() {
        rotate();
        long a = 0, l = 0;
        for (int i = 0; i < WINDOW_BUCKETS; i++) { a += acked[i]; l += lost[i]; }
        return a + l == 0 ? 0D : (double) l / (a + l);
    }

    private int recordLargeLoss(long now) {
        if (lastLargeLossNanos == 0 || now - lastLargeLossNanos > TimeUnit.SECONDS.toNanos(10)) {
            largeLosses = 0;
        }
        lastLargeLossNanos = now;
        return ++largeLosses;
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
            double target = packets / seconds * 1.25D;
            // Delivery-rate sampling prevents ACK compression from increasing
            // PPS faster than the path is actually delivering bytes.
            if (deliveryRateBytesPerSecond > 0) {
                final double averagePacketBytes = Math.max(64D, totalAckedBytes() / (double) packets);
                target = Math.min(target, deliveryRateBytesPerSecond / averagePacketBytes * 1.10D);
            }
            target = clampPps(target);
            packetsPerSecond = packetsPerSecond * 0.8D + target * 0.2D;
        }
        publishMetrics();
    }

    private boolean queueInflated() {
        return minRtt != Long.MAX_VALUE && smoothedRtt > minRtt * 2;
    }

    private void publishMetrics() {
        config.getMetrics().adaptivePacingRate(packetsPerSecond);
        config.getMetrics().adaptiveDeliveryRate(deliveryRateBytesPerSecond);
        long acknowledgements = 0, losses = 0;
        for (int i = 0; i < WINDOW_BUCKETS; i++) {
            acknowledgements += acked[i];
            losses += lost[i];
        }
        config.getMetrics().adaptiveLoss(
                acknowledgements + losses == 0 ? 0D : losses / (double) (acknowledgements + losses),
                acknowledgements, losses);
        config.getMetrics().adaptiveLossType(lossType.name());
    }

    int fecGroupSize() {
        return LimitedFecHandler.selectGroupSize(lossRatio());
    }

    private long totalAckedBytes() {
        long bytes = 0;
        for (long value : ackedBytes) bytes += value;
        return bytes;
    }

    private double clampPps(double value) {
        final int min = Math.max(1, config.getAdaptiveMinPps());
        final int max = Math.max(min, config.getAdaptiveMaxPps());
        return Math.max(min, Math.min(max, value));
    }
}
