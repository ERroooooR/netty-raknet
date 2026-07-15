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
    enum LossType { NONE, RANDOM, BURST, RATE_LIMIT, MTU_BLACK_HOLE, QUEUE }
    enum CongestionMode { STARTUP, DRAIN, PROBE_BW, PROBE_RTT }

    private static final long DSCP_COOLDOWN = TimeUnit.SECONDS.toNanos(30);
    private static final AtomicLong LAST_DSCP_CHANGE = new AtomicLong();
    private static final AtomicInteger CURRENT_TOS = new AtomicInteger(-1);
    private static final LongAdder HEALTHY_VOTES = new LongAdder();
    private static final LongAdder CONGESTED_VOTES = new LongAdder();
    private static final int WINDOW_BUCKETS = 10;
    private static final double[] PROBE_BW_GAINS = {1.25D, 0.75D, 1D, 1D, 1D, 1D, 1D, 1D};
    private static final long MIN_RTT_FILTER_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final long PROBE_RTT_NANOS = TimeUnit.MILLISECONDS.toNanos(200);
    private static final long MIN_CONGESTION_RESPONSE_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(250);
    private static final long MIN_LOSS_RECOVERY_QUIET_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final long MIN_BANDWIDTH_PROBE_COOLDOWN_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int BURST_DRAIN_ENTER_BYTES = 48 * 1024;
    private static final int BURST_DRAIN_EXIT_BYTES = 16 * 1024;
    private static final int BURST_DRAIN_MAX_PPS = 300;
    private static final double BURST_DRAIN_TARGET_SECONDS = 2D;

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
    private long lastLossNanos;
    private long lastCongestionResponseNanos;
    private long lossRecoveryUpdatedNanos;
    private double lossPacingCeiling = Double.POSITIVE_INFINITY;
    private long bandwidthProbeSuppressedUntil;
    private String congestionReason = "NONE";
    private boolean pacingCapped;
    private long pacingRateUpdatedNanos = System.nanoTime();
    private boolean burstDrainActive;
    private long burstDrainStartedNanos;
    private double burstDrainFloorPps;

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
        return sendBudget(nowNanos, inFlightBytes, nextDatagramBytes, 0);
    }

    int sendBudget(long nowNanos, long inFlightBytes, int nextDatagramBytes, int queuedBytes) {
        if (!config.isAdaptiveTransportEnabled()) return Integer.MAX_VALUE;
        final long outstandingBytes = Math.max(0L, inFlightBytes) + Math.max(0, queuedBytes);
        onOutstandingBytes(nowNanos, outstandingBytes);
        lastInFlightBytes = Math.max(0, inFlightBytes);
        updateMode(nowNanos, inFlightBytes);
        if (inFlightBytes > 0 && inFlightBytes + nextDatagramBytes > congestionWindowBytes) {
            nextSendNanos = nowNanos + Math.max(100_000L, smoothedRtt / 8L);
            publishCongestionMetrics();
            return 0;
        }
        final int burst = lossType == LossType.BURST || lossType == LossType.QUEUE
                || lossType == LossType.RATE_LIMIT ? 1 : 4;
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

    void onOutstandingBytes(long nowNanos, long outstandingBytes) {
        if (!config.isAdaptiveTransportEnabled()) return;
        final boolean wasActive = burstDrainActive;
        updateBurstDrain(nowNanos, outstandingBytes);
        if (wasActive != burstDrainActive) publishMetrics();
    }

    long nanosUntilSend(long nowNanos) {
        final long delay = Math.max(0, nextSendNanos - nowNanos);
        config.getMetrics().pacingDelay(delay);
        return delay;
    }

    boolean congestionWindowBlocked(long inFlightBytes, int nextDatagramBytes) {
        return config.isAdaptiveTransportEnabled() && inFlightBytes > 0
                && nextDatagramBytes > Math.max(0L, congestionWindowBytes - inFlightBytes);
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
        final double recentLoss = lossRatio();
        if (queueInflated() && recentLoss >= 0.01D) {
            lossType = LossType.QUEUE;
            congestionReason = "RTT_INFLATION_LOSS";
            enterCongestion(now);
        } else if (recentLoss < 0.005D && lossQuiet(now)) {
            lossType = LossType.NONE;
            congestionReason = "NONE";
        }
        updateCongestionModel(now, inFlightBytes, bytes);
        publishMetrics();
    }

    void onLoss(int bytes, boolean timeout) {
        if (!config.isAdaptiveTransportEnabled()) return;
        rotate();
        lost[bucket]++;
        consecutiveLosses++;
        final long now = System.nanoTime();
        lastLossNanos = now;
        if (timeout && !queueInflated() && (lossSampleCount() < 64 || lossRatio() < 0.02D)
                && bytes >= config.getMTU() - 64 && recordLargeLoss(now) >= 3) {
            lossType = LossType.MTU_BLACK_HOLE;
            congestionReason = "MTU_BLACK_HOLE";
            pathMtu.onBlackHole();
            largeLosses = 0;
        } else if (queueInflated() && (timeout || lossRatio() >= 0.01D)) {
            lossType = LossType.QUEUE;
            congestionReason = "RTT_INFLATION_LOSS";
        } else if (nonCongestiveHighLoss()) {
            lossType = LossType.RATE_LIMIT;
            congestionReason = "NON_CONGESTIVE_HIGH_LOSS";
        } else if (consecutiveLosses >= 3) {
            lossType = LossType.BURST;
            congestionReason = "CONSECUTIVE_LOSS";
        } else {
            lossType = LossType.RANDOM;
            congestionReason = "ISOLATED_LOSS";
        }
        final boolean severeLoss = lossType == LossType.QUEUE || lossType == LossType.BURST
                || lossType == LossType.RATE_LIMIT;
        final boolean newCongestionResponse = !severeLoss || enterCongestion(now);
        final double reduction = lossType == LossType.RATE_LIMIT ? 0.70D
                : lossType == LossType.QUEUE || lossType == LossType.BURST ? 0.75D : 0.85D;
        // A NACK range or one expired FrameSet can report many losses in the
        // same event-loop turn. The congestion window already responds at most
        // once per RTT; apply the pacing reduction at that same cadence instead
        // of multiplying the rate down to the configured floor immediately.
        if (newCongestionResponse) {
            packetsPerSecond = applyBurstDrainFloor(clampPps(packetsPerSecond * reduction));
            lossPacingCeiling = Math.min(lossPacingCeiling, packetsPerSecond);
            lossRecoveryUpdatedNanos = now;
        }
        publishMetrics();
    }

    void onEcnCe() {
        if (!config.isAdaptiveTransportEnabled()) return;
        rotate();
        ecnMarks[bucket]++;
        if (ecnCeRatio() >= 0.10D) {
            lossType = LossType.QUEUE;
            congestionReason = "ECN_CE";
            congestionMode = CongestionMode.DRAIN;
            congestionWindowBytes = Math.max(minimumCongestionWindow(), congestionWindowBytes * 7L / 8L);
            packetsPerSecond = clampPps(packetsPerSecond * 0.90D);
            final long now = System.nanoTime();
            lastLossNanos = now;
            lossPacingCeiling = Math.min(lossPacingCeiling, packetsPerSecond);
            lossRecoveryUpdatedNanos = now;
        }
        publishMetrics();
    }

    LossType lossType() { return lossType; }
    CongestionMode congestionMode() { return congestionMode; }
    long congestionWindowBytes() { return congestionWindowBytes; }
    double packetsPerSecond() { return packetsPerSecond; }

    boolean shouldUseFec() {
        final double ratio = lossRatio();
        return config.isAdaptiveTransportEnabled() && lossType == LossType.RANDOM
                && ratio >= 0.005D && ratio <= 0.03D;
    }

    int smallWriteCoalesceMicros() {
        final int base = config.getSmallWriteCoalesceMicros();
        if (!config.isAdaptiveTransportEnabled()) return base;
        if (lossType == LossType.RATE_LIMIT) return Math.max(base, 1_500);
        if (lossType == LossType.QUEUE || lossType == LossType.BURST) return Math.max(base, 750);
        return base;
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
            if (lossType == LossType.BURST || lossType == LossType.QUEUE
                    || lossType == LossType.RATE_LIMIT) CONGESTED_VOTES.increment();
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
        final double gain = pacingGain(now);
        if (bandwidth > 0) {
            final double modelRate = (bandwidth * gain) / Math.max(64D, averagePacketBytes);
            final double lossLimitedRate = applyLossPacingCeiling(now, modelRate);
            packetsPerSecond = applyBurstDrainFloor(clampPps(applyPacingSlew(now, lossLimitedRate)));
        }
    }

    private void updateBurstDrain(long now, long outstandingBytes) {
        if (burstDrainActive) {
            if (outstandingBytes <= BURST_DRAIN_EXIT_BYTES) {
                burstDrainActive = false;
                burstDrainStartedNanos = 0;
                burstDrainFloorPps = 0D;
                return;
            }
        } else if (outstandingBytes >= BURST_DRAIN_ENTER_BYTES) {
            burstDrainActive = true;
            burstDrainStartedNanos = now;
        } else {
            return;
        }

        final double estimatedPayloadBytes = Math.max(512D, config.getMTU() * 0.75D);
        double target = outstandingBytes / estimatedPayloadBytes / BURST_DRAIN_TARGET_SECONDS;
        target = Math.max(100D, target);
        final double recentLoss = lossRatio();
        if (recentLoss >= 0.10D) target *= 0.50D;
        else if (recentLoss >= 0.05D) target *= 0.75D;
        if (minRtt != Long.MAX_VALUE && smoothedRtt >= minRtt * 2L) target *= 0.85D;
        if (recentLoss < 0.10D) target = Math.max(100D, target);
        final double ceiling = Math.min(BURST_DRAIN_MAX_PPS, config.getAdaptiveMaxPps());
        burstDrainFloorPps = Math.max(config.getAdaptiveMinPps(), Math.min(ceiling, target));
        packetsPerSecond = applyBurstDrainFloor(packetsPerSecond);
        if (!Double.isInfinite(lossPacingCeiling)) {
            lossPacingCeiling = Math.max(lossPacingCeiling, burstDrainFloorPps);
        }
    }

    private double applyBurstDrainFloor(double rate) {
        return burstDrainActive ? Math.max(rate, burstDrainFloorPps) : rate;
    }

    private double applyPacingSlew(long now, double targetRate) {
        if (targetRate <= packetsPerSecond) {
            pacingRateUpdatedNanos = now;
            return targetRate;
        }
        final long elapsed = Math.min(TimeUnit.SECONDS.toNanos(1),
                Math.max(0L, now - pacingRateUpdatedNanos));
        pacingRateUpdatedNanos = now;
        // In steady state, at most double the pacing rate per second. This
        // filters ACK-compression spikes without delaying immediate reductions.
        final double growthExponent = elapsed / 1_000_000_000D;
        return Math.min(targetRate, packetsPerSecond * Math.pow(2D, growthExponent));
    }

    private boolean enterCongestion(long now) {
        congestionMode = CongestionMode.DRAIN;
        final long responseInterval = Math.max(MIN_CONGESTION_RESPONSE_INTERVAL_NANOS, smoothedRtt);
        if (lastCongestionResponseNanos != 0 && now - lastCongestionResponseNanos < responseInterval) return false;
        lastCongestionResponseNanos = now;
        bandwidthProbeSuppressedUntil = Math.max(bandwidthProbeSuppressedUntil,
                now + Math.max(MIN_BANDWIDTH_PROBE_COOLDOWN_NANOS,
                        saturatedMultiply(smoothedRtt, 8L)));
        gainCycle = 2; // resume at 1.0 rather than immediately probing at 1.25
        congestionWindowBytes = Math.max(minimumCongestionWindow(), congestionWindowBytes * 3L / 4L);
        // A policer/queue event makes the old max-bandwidth samples unsafe. Keep
        // some history for quick recovery, but do not let the next ACK restore
        // the exact pre-loss pacing rate immediately.
        for (int i = 0; i < bandwidthFilter.length; i++) {
            bandwidthFilter[i] = bandwidthFilter[i] * 3L / 4L;
        }
        deliveryRateBytesPerSecond = maxBandwidth();
        return true;
    }

    private double applyLossPacingCeiling(long now, double modelRate) {
        if (Double.isInfinite(lossPacingCeiling)) {
            pacingCapped = false;
            return modelRate;
        }
        final long quietPeriod = Math.max(MIN_LOSS_RECOVERY_QUIET_NANOS,
                saturatedMultiply(smoothedRtt, 2L));
        if (now - lastLossNanos > quietPeriod) {
            final long elapsed = Math.max(0L, now - lossRecoveryUpdatedNanos);
            if (elapsed > 0) {
                // Double the allowed rate per second after a quiet period. This
                // cannot jump back to a stale pre-QoS estimate on the first ACK.
                lossPacingCeiling *= Math.pow(2D, elapsed / 1_000_000_000D);
                lossRecoveryUpdatedNanos = now;
            }
            if (lossPacingCeiling >= modelRate || (lossRatio() < 0.005D
                    && now - lastLossNanos > TimeUnit.SECONDS.toNanos(5))) {
                lossPacingCeiling = Double.POSITIVE_INFINITY;
                pacingCapped = false;
                return modelRate;
            }
        }
        pacingCapped = modelRate > lossPacingCeiling;
        return Math.min(modelRate, lossPacingCeiling);
    }

    private boolean lossQuiet(long now) {
        return lastLossNanos == 0 || now - lastLossNanos
                > Math.max(TimeUnit.SECONDS.toNanos(1), saturatedMultiply(smoothedRtt, 4L));
    }

    private void updateMode(long now, long inFlightBytes) {
        final long rtt = minRtt == Long.MAX_VALUE ? Math.max(1, smoothedRtt) : minRtt;
        final long bdp = Math.max(minimumCongestionWindow(), saturatedMultiply(maxBandwidth(), rtt) / 1_000_000_000L);
        if (minRttStamp != 0 && now - minRttStamp > MIN_RTT_FILTER_NANOS
                && canProbeRtt(now)
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

    private boolean canProbeRtt(long now) {
        return lossType == LossType.NONE && lossRatio() < 0.005D && lossQuiet(now)
                && now >= bandwidthProbeSuppressedUntil;
    }

    private double pacingGain(long now) {
        if (congestionMode == CongestionMode.STARTUP) return 2D;
        if (congestionMode == CongestionMode.DRAIN) return 0.75D;
        if (congestionMode == CongestionMode.PROBE_RTT) return 0.5D;
        final double gain = PROBE_BW_GAINS[gainCycle];
        return now < bandwidthProbeSuppressedUntil ? Math.min(1D, gain) : gain;
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

    private boolean queueInflated() {
        return minRtt != Long.MAX_VALUE && smoothedRtt > minRtt * 3L / 2L;
    }
    private boolean nonCongestiveHighLoss() {
        return lossSampleCount() >= 64 && lossRatio() >= 0.03D
                && minRtt != Long.MAX_VALUE && smoothedRtt < minRtt * 3L / 2L;
    }
    private long lossSampleCount() {
        long samples = 0;
        for (int i = 0; i < WINDOW_BUCKETS; i++) samples += acked[i] + lost[i];
        return samples;
    }
    private long minimumCongestionWindow() { return Math.max(4L * config.getMTU(), 4L * 576L); }
    private long maximumCongestionWindow() { return Math.max(minimumCongestionWindow(), config.getMaxQueuedBytes()); }

    private void publishMetrics() {
        config.getMetrics().adaptivePacingRate(packetsPerSecond);
        // Byte pacing and adaptive ACK policy remain disabled after the public-network
        // rollback. Keep publishing neutral values so telemetry remains schema-stable.
        config.getMetrics().adaptiveBytePacingRate(0L);
        config.getMetrics().adaptiveAckPolicy(false, 0L, 0L);
        config.getMetrics().adaptiveDemand(!burstDrainActive, burstDrainActive ? "BULK" : "IDLE",
                burstDrainActive ? Math.max(0L, System.nanoTime() - burstDrainStartedNanos) : 0L, 0L);
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
        final double inflation = minRtt == Long.MAX_VALUE || minRtt <= 0 ? 1D
                : Math.max(1D, smoothedRtt / (double) minRtt);
        config.getMetrics().congestionDiagnostics(congestionReason, inflation, pacingCapped,
                System.nanoTime() < bandwidthProbeSuppressedUntil);
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
