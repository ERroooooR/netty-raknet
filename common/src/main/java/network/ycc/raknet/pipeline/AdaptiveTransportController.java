package network.ycc.raknet.pipeline;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import network.ycc.raknet.RakNet;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Event-loop confined model-based congestion, loss and DPLPMTUD controller. */
final class AdaptiveTransportController {
    enum LossType { NONE, RANDOM, BURST, MTU_BLACK_HOLE, QUEUE }
    enum CongestionMode { STARTUP, DRAIN, PROBE_BW, PROBE_RTT }

    private static final long DSCP_COOLDOWN = TimeUnit.SECONDS.toNanos(30);
    private static final AtomicLong LAST_DSCP_CHANGE = new AtomicLong();
    private static final AtomicInteger CURRENT_TOS = new AtomicInteger(-1);
    private static final LongAdder HEALTHY_VOTES = new LongAdder();
    private static final LongAdder CONGESTED_VOTES = new LongAdder();
    private static final int WINDOW_BUCKETS = 10;
    private static final double[] PROBE_BW_GAINS = {1.25D, 0.75D, 1D, 1D, 1D, 1D, 1D, 1D};
    private static final long MIN_RTT_FILTER_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long PROBE_RTT_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

    private final RakNet.Config config;
    private DplpmtudController pathMtu;
    private long nextSendNanos;
    private long tokenUpdatedNanos;
    private double pacingTokens = 1D;
    private final long[] acked = new long[WINDOW_BUCKETS];
    private final long[] lost = new long[WINDOW_BUCKETS];
    private final long[] ackedBytes = new long[WINDOW_BUCKETS];
    private final long[] bandwidthFilter = new long[WINDOW_BUCKETS];
    private final long[] ecnMarks = new long[WINDOW_BUCKETS];
    private int bucket;
    private long bucketStarted = System.nanoTime();
    private int consecutiveLosses;
    private int largeLosses;
    private long lastLargeLossNanos;
    private LossType lossType = LossType.NONE;
    private CongestionMode congestionMode = CongestionMode.STARTUP;
    private double packetsPerSecond;
    private long minRtt = Long.MAX_VALUE;
    private long minRttStamp;
    private long smoothedRtt;
    private long deliveryRateBytesPerSecond;
    private long deliverySampleStarted;
    private long deliverySampleBytes;
    private long lastDscpVote;
    private double averagePacketBytes;
    private long congestionWindowBytes;
    private long ackEpochStart;
    private long ackEpochBytes;
    private long ackAggregationBytes;
    private long fullBandwidth;
    private int fullBandwidthRounds;
    private long roundStarted;
    private long probeRttStarted;
    private int gainCycle;
    private long gainCycleStarted;
    private long lastInFlightBytes;

    AdaptiveTransportController(RakNet.Config config) {
        this.config = config;
        this.pathMtu = new DplpmtudController(config.getMTU(), config.getPlpmtudMaxMtu());
        this.packetsPerSecond = clampPps(500D);
        this.averagePacketBytes = Math.max(256D, config.getMTU() * 0.75D);
        this.congestionWindowBytes = minimumCongestionWindow();
        config.getMetrics().adaptiveMTU(pathMtu.confirmedMtu());
        publishMetrics();
    }

    void onNegotiatedMtu(int mtu) {
        pathMtu = new DplpmtudController(mtu, config.getPlpmtudMaxMtu());
        congestionWindowBytes = minimumCongestionWindow();
        averagePacketBytes = Math.max(256D, mtu * 0.75D);
        config.getMetrics().adaptiveMTU(mtu);
        publishPathMtuMetrics();
    }

    int sendBudget(long nowNanos) {
        return sendBudget(nowNanos, 0, config.getMTU());
    }

    int sendBudget(long nowNanos, long inFlightBytes, int nextDatagramBytes) {
        if (!config.isAdaptiveTransportEnabled()) return Integer.MAX_VALUE;
        lastInFlightBytes = Math.max(0, inFlightBytes);
        updateMode(nowNanos, inFlightBytes);
        if (inFlightBytes > 0 && inFlightBytes + nextDatagramBytes > congestionWindowBytes) {
            nextSendNanos = nowNanos + Math.max(100_000L, smoothedRtt / 8L);
            publishCongestionMetrics();
            return 0;
        }
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
            final long cwndBudget = Math.max(1L,
                    (congestionWindowBytes - inFlightBytes + nextDatagramBytes - 1L) / nextDatagramBytes);
            budget = (int) Math.min(budget, cwndBudget);
            pacingTokens -= budget;
        } else if (pacingTokens >= 0D && inFlightBytes == 0) {
            budget = 1;
            pacingTokens -= 1D;
        }
        final double missing = Math.max(0D, -pacingTokens);
        nextSendNanos = nowNanos + Math.max(10_000L,
                (long) Math.ceil(missing * 1_000_000_000D / packetsPerSecond));
        publishCongestionMetrics();
        return budget;
    }

    long nanosUntilSend(long nowNanos) {
        final long delay = Math.max(0, nextSendNanos - nowNanos);
        config.getMetrics().pacingDelay(delay);
        return delay;
    }

    void onAck(int bytes, long rttNanos) {
        onAck(bytes, rttNanos, lastInFlightBytes);
    }

    void onAck(int bytes, long rttNanos, long inFlightBytes) {
        if (!config.isAdaptiveTransportEnabled()) return;
        rotate();
        acked[bucket]++;
        ackedBytes[bucket] += bytes;
        final long now = System.nanoTime();
        averagePacketBytes = averagePacketBytes * 0.875D + bytes * 0.125D;
        updateDeliveryRate(now, bytes);
        updateAckAggregation(now, bytes);
        if (rttNanos > 0) {
            if (rttNanos < minRtt || minRttStamp == 0) {
                minRtt = rttNanos;
                minRttStamp = now;
            }
            smoothedRtt = smoothedRtt == 0 ? rttNanos : (smoothedRtt * 7 + rttNanos) / 8;
        }
        consecutiveLosses = 0;
        if (bytes >= config.getMTU() - 64) {
            largeLosses = 0;
            lastLargeLossNanos = 0;
        }
        if (lossRatio() < 0.01D) lossType = LossType.NONE;
        updateCongestionModel(now, inFlightBytes, bytes);
        publishMetrics();
    }

    void onLoss(int bytes, boolean timeout) {
        if (!config.isAdaptiveTransportEnabled()) return;
        rotate();
        lost[bucket]++;
        consecutiveLosses++;
        final long now = System.nanoTime();
        if (bytes >= config.getMTU() - 64 && recordLargeLoss(now) >= 3) {
            lossType = LossType.MTU_BLACK_HOLE;
            pathMtu.onBlackHole();
            largeLosses = 0;
        } else if (timeout && (lossRatio() > 0.05D || queueInflated())) {
            lossType = LossType.QUEUE;
        } else if (consecutiveLosses >= 3) {
            lossType = LossType.BURST;
        } else {
            lossType = LossType.RANDOM;
        }
        if (lossType == LossType.QUEUE || lossType == LossType.BURST) {
            congestionWindowBytes = Math.max(minimumCongestionWindow(), congestionWindowBytes * 3L / 4L);
            congestionMode = CongestionMode.DRAIN;
        }
        packetsPerSecond = clampPps(packetsPerSecond * 0.85D);
        publishMetrics();
    }

    void onEcnCe() {
        if (!config.isAdaptiveTransportEnabled()) return;
        rotate();
        ecnMarks[bucket]++;
        if (ecnCeRatio() >= 0.10D) {
            lossType = LossType.QUEUE;
            congestionMode = CongestionMode.DRAIN;
            congestionWindowBytes = Math.max(minimumCongestionWindow(), congestionWindowBytes * 7L / 8L);
            packetsPerSecond = clampPps(packetsPerSecond * 0.90D);
        }
        publishMetrics();
    }

    LossType lossType() { return lossType; }
    CongestionMode congestionMode() { return congestionMode; }
    long congestionWindowBytes() { return congestionWindowBytes; }

    boolean shouldUseFec() {
        final double ratio = lossRatio();
        return config.isAdaptiveTransportEnabled() && lossType == LossType.RANDOM
                && ratio >= 0.005D && ratio <= 0.15D;
    }

    int probeCandidate() { return pathMtu.nextProbe(System.nanoTime()); }

    void onProbeSent(int mtu) {
        pathMtu.onProbeSent(mtu);
        publishPathMtuMetrics();
    }

    void onProbeAck(int mtu) {
        pathMtu.onProbeAcknowledged(mtu, System.nanoTime());
        publishPathMtuMetrics();
    }

    void onProbeTimeout(int mtu) {
        pathMtu.onProbeTimeout(mtu, System.nanoTime());
        config.getMetrics().pathMtuProbeResult("timeout", mtu);
        publishPathMtuMetrics();
    }

    void onPacketTooBig(int reportedMtu, int triggeringDatagramSize) {
        pathMtu.onPacketTooBig(reportedMtu, triggeringDatagramSize);
        applyPendingMtu();
        config.getMetrics().pathMtuProbeResult("packet_too_big", reportedMtu);
        publishPathMtuMetrics();
    }

    void onLocalMessageTooLong(int triggeringDatagramSize) {
        pathMtu.onLocalMessageTooLong(triggeringDatagramSize);
        config.getMetrics().pathMtuProbeResult("local_message_too_long", triggeringDatagramSize);
        publishPathMtuMetrics();
    }

    boolean shouldRetryProbe() {
        return pathMtu.probedMtu() != 0 && pathMtu.probeCount() < DplpmtudController.MAX_PROBES;
    }

    void applyPendingMtu() {
        final int mtu = pathMtu.confirmedMtu();
        if (config.getMTU() != mtu) {
            config.setMTU(mtu);
            config.getMetrics().adaptiveMTU(mtu);
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
        final int tos = congested > healthy * 2 ? 0x00 : healthy > congested * 2 ? 0x88 : CURRENT_TOS.get();
        if (tos >= 0 && CURRENT_TOS.get() != tos && LAST_DSCP_CHANGE.compareAndSet(previous, now)) {
            if (channel.config().setOption(ChannelOption.IP_TOS, tos)) {
                CURRENT_TOS.set(tos);
                config.getMetrics().adaptiveDscp(tos);
            }
        }
    }

    FecParameters fecParameters() {
        final double loss = lossRatio();
        if (loss < 0.02D) return new FecParameters(12, 1, 0.08D);
        if (loss < 0.05D) return new FecParameters(10, 1, 0.10D);
        if (loss < 0.08D) return new FecParameters(8, 1, 0.12D);
        return new FecParameters(8, 2, 0.20D);
    }

    int fecGroupSize() { return fecParameters().dataShards; }

    private void updateCongestionModel(long now, long inFlightBytes, int acknowledgedBytes) {
        final long bandwidth = maxBandwidth();
        final long rtt = minRtt == Long.MAX_VALUE ? Math.max(1, smoothedRtt) : minRtt;
        final long bdp = Math.max(minimumCongestionWindow(), saturatedMultiply(bandwidth, rtt) / 1_000_000_000L);
        if (roundStarted == 0 || now - roundStarted >= Math.max(1, smoothedRtt)) {
            if (bandwidth >= fullBandwidth + Math.max(1, fullBandwidth / 4)) {
                fullBandwidth = bandwidth;
                fullBandwidthRounds = 0;
            } else if (++fullBandwidthRounds >= 3 && congestionMode == CongestionMode.STARTUP) {
                congestionMode = CongestionMode.DRAIN;
            }
            roundStarted = now;
        }
        if (congestionMode == CongestionMode.STARTUP) {
            congestionWindowBytes = Math.min(maximumCongestionWindow(), congestionWindowBytes + acknowledgedBytes);
        } else {
            final long target = Math.min(maximumCongestionWindow(), saturatedAdd(bdp * 2L, ackAggregationBytes));
            congestionWindowBytes = Math.max(minimumCongestionWindow(),
                    (congestionWindowBytes * 7L + target) / 8L);
        }
        updateMode(now, inFlightBytes);
        final double gain = pacingGain();
        if (bandwidth > 0) {
            packetsPerSecond = clampPps((bandwidth * gain) / Math.max(64D, averagePacketBytes));
        }
    }

    private void updateMode(long now, long inFlightBytes) {
        final long rtt = minRtt == Long.MAX_VALUE ? Math.max(1, smoothedRtt) : minRtt;
        final long bdp = Math.max(minimumCongestionWindow(), saturatedMultiply(maxBandwidth(), rtt) / 1_000_000_000L);
        if (minRttStamp != 0 && now - minRttStamp > MIN_RTT_FILTER_NANOS
                && congestionMode != CongestionMode.PROBE_RTT) {
            congestionMode = CongestionMode.PROBE_RTT;
            probeRttStarted = 0;
        }
        if (congestionMode == CongestionMode.DRAIN && inFlightBytes <= bdp) {
            congestionMode = CongestionMode.PROBE_BW;
            gainCycleStarted = now;
        } else if (congestionMode == CongestionMode.PROBE_BW && now - gainCycleStarted >= Math.max(1, rtt)) {
            gainCycle = (gainCycle + 1) % PROBE_BW_GAINS.length;
            gainCycleStarted = now;
        } else if (congestionMode == CongestionMode.PROBE_RTT) {
            congestionWindowBytes = minimumCongestionWindow();
            if (inFlightBytes <= minimumCongestionWindow()) {
                if (probeRttStarted == 0) probeRttStarted = now;
                else if (now - probeRttStarted >= PROBE_RTT_NANOS) {
                    minRttStamp = now;
                    congestionMode = CongestionMode.PROBE_BW;
                    gainCycleStarted = now;
                }
            }
        }
    }

    private double pacingGain() {
        if (congestionMode == CongestionMode.STARTUP) return 2D;
        if (congestionMode == CongestionMode.DRAIN) return 0.75D;
        if (congestionMode == CongestionMode.PROBE_RTT) return 0.5D;
        return PROBE_BW_GAINS[gainCycle];
    }

    private void updateAckAggregation(long now, int bytes) {
        if (ackEpochStart == 0 || now - ackEpochStart > Math.max(TimeUnit.SECONDS.toNanos(1), smoothedRtt)) {
            ackEpochStart = now;
            ackEpochBytes = 0;
        }
        ackEpochBytes += bytes;
        final long expected = saturatedMultiply(maxBandwidth(), now - ackEpochStart) / 1_000_000_000L;
        final long excess = Math.max(0, ackEpochBytes - expected);
        ackAggregationBytes = Math.min(minimumCongestionWindow() * 4L,
                Math.max(ackAggregationBytes * 7L / 8L, excess));
    }

    private void updateDeliveryRate(long now, int bytes) {
        if (deliverySampleStarted == 0) deliverySampleStarted = now;
        deliverySampleBytes += bytes;
        final long interval = now - deliverySampleStarted;
        final long samplingWindow = Math.max(TimeUnit.MILLISECONDS.toNanos(1),
                Math.min(TimeUnit.MILLISECONDS.toNanos(100), Math.max(1, smoothedRtt / 2L)));
        if (interval < samplingWindow) return;
        final long sample = saturatedMultiply(deliverySampleBytes, 1_000_000_000L) / interval;
        bandwidthFilter[bucket] = Math.max(bandwidthFilter[bucket], sample);
        deliveryRateBytesPerSecond = maxBandwidth();
        deliverySampleStarted = now;
        deliverySampleBytes = 0;
    }

    private double lossRatio() {
        rotate();
        long a = 0, l = 0;
        for (int i = 0; i < WINDOW_BUCKETS; i++) { a += acked[i]; l += lost[i]; }
        return a + l == 0 ? 0D : (double) l / (a + l);
    }

    private double ecnCeRatio() {
        long a = 0, ce = 0;
        for (int i = 0; i < WINDOW_BUCKETS; i++) { a += acked[i]; ce += ecnMarks[i]; }
        return a + ce == 0 ? 0D : ce / (double) (a + ce);
    }

    private int recordLargeLoss(long now) {
        if (lastLargeLossNanos == 0 || now - lastLargeLossNanos > TimeUnit.SECONDS.toNanos(10)) largeLosses = 0;
        lastLargeLossNanos = now;
        return ++largeLosses;
    }

    private void rotate() {
        final long now = System.nanoTime();
        long elapsed = (now - bucketStarted) / TimeUnit.SECONDS.toNanos(1);
        if (elapsed >= WINDOW_BUCKETS) {
            Arrays.fill(acked, 0); Arrays.fill(lost, 0); Arrays.fill(ackedBytes, 0);
            Arrays.fill(bandwidthFilter, 0); Arrays.fill(ecnMarks, 0);
            bucket = 0; bucketStarted = now; return;
        }
        while (elapsed-- > 0) {
            bucket = (bucket + 1) % WINDOW_BUCKETS;
            acked[bucket] = lost[bucket] = ackedBytes[bucket] = bandwidthFilter[bucket] = ecnMarks[bucket] = 0;
            bucketStarted += TimeUnit.SECONDS.toNanos(1);
        }
    }

    private long maxBandwidth() {
        long max = 0;
        for (long value : bandwidthFilter) max = Math.max(max, value);
        return max;
    }

    private boolean queueInflated() { return minRtt != Long.MAX_VALUE && smoothedRtt > minRtt * 2; }
    private long minimumCongestionWindow() { return Math.max(4L * config.getMTU(), 4L * 576L); }
    private long maximumCongestionWindow() { return Math.max(minimumCongestionWindow(), config.getMaxQueuedBytes()); }

    private void publishMetrics() {
        config.getMetrics().adaptivePacingRate(packetsPerSecond);
        config.getMetrics().adaptiveDeliveryRate(deliveryRateBytesPerSecond);
        long acknowledgements = 0, losses = 0;
        for (int i = 0; i < WINDOW_BUCKETS; i++) { acknowledgements += acked[i]; losses += lost[i]; }
        config.getMetrics().adaptiveLoss(acknowledgements + losses == 0 ? 0D
                : losses / (double) (acknowledgements + losses), acknowledgements, losses);
        config.getMetrics().adaptiveLossType(lossType.name());
        publishCongestionMetrics();
        publishPathMtuMetrics();
    }

    private void publishCongestionMetrics() {
        config.getMetrics().congestionControl(congestionMode.name(), congestionWindowBytes,
                lastInFlightBytes, maxBandwidth(), ackAggregationBytes, ecnCeRatio());
    }

    private void publishPathMtuMetrics() {
        config.getMetrics().pathMtuState(pathMtu.state().name(), pathMtu.confirmedMtu(),
                pathMtu.probedMtu(), pathMtu.maximumMtu());
    }

    private double clampPps(double value) {
        final int min = Math.max(1, config.getAdaptiveMinPps());
        final int max = Math.max(min, config.getAdaptiveMaxPps());
        return Math.max(min, Math.min(max, value));
    }

    private static long saturatedAdd(long a, long b) {
        return a > Long.MAX_VALUE - b ? Long.MAX_VALUE : a + b;
    }

    private static long saturatedMultiply(long a, long b) {
        if (a <= 0 || b <= 0) return 0;
        return a > Long.MAX_VALUE / b ? Long.MAX_VALUE : a * b;
    }

    static final class FecParameters {
        final int dataShards;
        final int parityShards;
        final double overheadBudget;

        FecParameters(int dataShards, int parityShards, double overheadBudget) {
            this.dataShards = dataShards;
            this.parityShards = parityShards;
            this.overheadBudget = overheadBudget;
        }
    }
}
