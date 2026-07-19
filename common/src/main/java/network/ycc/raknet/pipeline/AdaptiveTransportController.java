package network.ycc.raknet.pipeline;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import network.ycc.raknet.RakNet;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static network.ycc.raknet.utils.SaturatedMath.add;
import static network.ycc.raknet.utils.SaturatedMath.multiply;

/** Event-loop confined model-based congestion, loss and DPLPMTUD controller. */
final class AdaptiveTransportController {
    enum LossType { NONE, RANDOM, BURST, RATE_LIMIT, MTU_BLACK_HOLE, QUEUE }
    enum CongestionMode { STARTUP, DRAIN, PROBE_BW, PROBE_RTT }
    enum ResumeState { IDLE, VALIDATED, UNVALIDATED, SAFE_RETREAT }

    private static final long DSCP_COOLDOWN = TimeUnit.SECONDS.toNanos(30);
    private static final AtomicLong LAST_DSCP_CHANGE = new AtomicLong();
    private static final AtomicInteger CURRENT_TOS = new AtomicInteger(-1);
    private static final LongAdder HEALTHY_VOTES = new LongAdder();
    private static final LongAdder CONGESTED_VOTES = new LongAdder();
    // Minecraft chunk/ZSTD production is often application-limited for several
    // complete gain cycles. Keep a longer clean-path maximum so those gaps do
    // not erase capacity that was just demonstrated. Congestion responses
    // explicitly reduce every slot, so this does not defer loss feedback.
    private static final int BANDWIDTH_FILTER_CYCLES = 10;
    private static final double[] PROBE_BW_GAINS = {1.25D, 0.75D, 1D, 1D, 1D, 1D, 1D, 1D};
    private static final long MIN_RTT_FILTER_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final long PROBE_RTT_NANOS = TimeUnit.MILLISECONDS.toNanos(200);
    private static final long MIN_CONGESTION_RESPONSE_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(250);
    private static final long MIN_BANDWIDTH_PROBE_COOLDOWN_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int BURST_DRAIN_ENTER_BYTES = 48 * 1024;
    private static final int BURST_DRAIN_EXIT_BYTES = 16 * 1024;
    private static final int DEGRADED_BURST_MAX_PPS = 600;
    private static final int QOS_RECOVERY_BASE_PPS = 100;
    private static final long MIN_QOS_RECOVERY_STEP_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long MIN_BURST_DRAIN_HORIZON_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long MAX_BURST_DRAIN_HORIZON_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final int BURST_DRAIN_RTT_MULTIPLIER = 4;
    private static final long STARTUP_CALIBRATION_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final long BURST_ADMISSION_INITIAL_BPS = 384L * 1024L;
    private static final double BURST_ADMISSION_GROWTH = 1.25D;
    private static final double VALIDATED_CAPACITY_FLOOR = 0.80D;
    private static final int BURST_ADMISSION_TOKEN_DATAGRAMS = 4;
    private static final long BURST_ADMISSION_MAX_GROWTH_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(250);
    private static final long APPLICATION_ARRIVAL_TIME_CONSTANT_NANOS = TimeUnit.SECONDS.toNanos(2);
    private static final double APPLICATION_ARRIVAL_HEADROOM = 1.10D;
    private static final long VALIDATED_PATH_LIFETIME_NANOS = TimeUnit.MINUTES.toNanos(5);
    private static final int RESUME_VALIDATION_ROUNDS = 2;

    private final RakNet.Config config;
    private DplpmtudController pathMtu;
    private long nextSendNanos;
    private long tokenUpdatedNanos;
    private double pacingTokens = 1D;
    private final TransportLossWindow lossWindow = new TransportLossWindow();
    private final long[] bandwidthFilter = new long[BANDWIDTH_FILTER_CYCLES];
    private int bandwidthCycle;
    private boolean bandwidthCycleHadValidSample;
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
    private boolean deliverySampleApplicationLimited;
    private boolean lastDeliverySampleApplicationLimited;
    private long lastValidatedBandwidthNanos;
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
    private boolean congestionEpisodeActive;
    private long lossRecoveryUpdatedNanos;
    private double lossPacingCeiling = Double.POSITIVE_INFINITY;
    private long bandwidthProbeSuppressedUntil;
    private String congestionReason = "NONE";
    private boolean pacingCapped;
    private long pacingRateUpdatedNanos = System.nanoTime();
    private boolean burstDrainActive;
    private long burstDrainStartedNanos;
    private double burstDrainFloorPps;
    private long burstAdmissionRateBytesPerSecond;
    private double burstAdmissionTokens;
    private long burstAdmissionUpdatedNanos;
    private long burstRampUpdatedNanos;
    private long burstAdmissionTargetBytesPerSecond;
    private double applicationArrivalRateBytesPerSecond;
    private long applicationArrivalRateUpdatedNanos;
    private long lastAckNanos;
    private long burstRecoveryProbes;
    private ResumeState resumeState = ResumeState.IDLE;
    private long resumeStartedNanos;
    private long resumeValidationRoundBytes;
    private long resumeValidationAckedBytes;
    private int resumeValidatedRounds;
    private long unvalidatedBandwidthSample;
    private long scheduledPacingDeadlineNanos;
    private long pacerWakeupLatenessNanos;
    private boolean rttPressureActive;
    private long calibrationStartedNanos;

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
        if (scheduledPacingDeadlineNanos != 0L) {
            pacerWakeupLatenessNanos = Math.max(0L, nowNanos - scheduledPacingDeadlineNanos);
            scheduledPacingDeadlineNanos = 0L;
        }
        final long outstandingBytes = Math.max(0L, inFlightBytes) + Math.max(0, queuedBytes);
        onOutstandingBytes(nowNanos, outstandingBytes);
        updateBurstAdmission(nowNanos, nextDatagramBytes);
        final boolean workConserving = workConservingBulk(nowNanos);
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
        } else if (pacingTokens >= 0D && inFlightBytes == 0 && !burstDrainActive) {
            budget = 1;
        }
        if (burstDrainActive && burstAdmissionRateBytesPerSecond > 0 && !workConserving) {
            final int byteBudget = Math.max(0, (int) (burstAdmissionTokens / nextDatagramBytes));
            budget = Math.min(budget, byteBudget);
        }
        if (budget > 0) {
            pacingTokens -= budget;
            if (burstDrainActive && !workConserving) {
                burstAdmissionTokens -= (double) budget * nextDatagramBytes;
            }
        }
        // Preserve the existing one-packet idle credit outside the byte-gated
        // backlog path; the burst admission bucket is what smooths bulk starts.
        final double missingPackets = Math.max(0D, -pacingTokens);
        long pacingDelay = (long) Math.ceil(missingPackets * 1_000_000_000D / packetsPerSecond);
        if (burstDrainActive && burstAdmissionRateBytesPerSecond > 0 && !workConserving) {
            final double missingBytes = Math.max(0D, nextDatagramBytes - burstAdmissionTokens);
            pacingDelay = Math.max(pacingDelay, (long) Math.ceil(missingBytes * 1_000_000_000D
                    / burstAdmissionRateBytesPerSecond));
        }
        nextSendNanos = nowNanos + Math.max(10_000L, pacingDelay);
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
        scheduledPacingDeadlineNanos = nextSendNanos;
        config.getMetrics().pacingDelay(delay);
        return delay;
    }

    void onPacingBatchSent(int datagrams) {
        if (!config.isAdaptiveTransportEnabled() || datagrams <= 0) return;
        config.getMetrics().pacingScheduler(pacerWakeupLatenessNanos, datagrams);
        pacerWakeupLatenessNanos = 0L;
    }

    boolean congestionWindowBlocked(long inFlightBytes, int nextDatagramBytes) {
        return config.isAdaptiveTransportEnabled() && inFlightBytes > 0
                && nextDatagramBytes > Math.max(0L, congestionWindowBytes - inFlightBytes);
    }

    boolean allowsApplicationLimitedRecovery() {
        return !config.isAdaptiveTransportEnabled()
                || (!burstDrainActive && !queueInflated()
                && lossType != LossType.QUEUE && lossType != LossType.MTU_BLACK_HOLE);
    }

    void onAck(int bytes, long rttNanos) {
        onAck(bytes, rttNanos, lastInFlightBytes);
    }

    void onAck(int bytes, long rttNanos, long inFlightBytes) {
        onAck(bytes, rttNanos, inFlightBytes, false, Long.MIN_VALUE);
    }

    void onAck(int bytes, long rttNanos, long inFlightBytes,
               boolean applicationLimited, long packetSentNanos) {
        if (!config.isAdaptiveTransportEnabled()) return;
        final long now = System.nanoTime();
        lossWindow.recordAcknowledgement(now);
        final long ackGap = lastAckNanos == 0L ? 0L : Math.max(0L, now - lastAckNanos);
        final long idleThreshold = Math.max(TimeUnit.MILLISECONDS.toNanos(250),
                multiply(Math.max(1L, smoothedRtt), 2L));
        final boolean idleRestartSample = ackGap > idleThreshold;
        averagePacketBytes = averagePacketBytes * 0.875D + bytes * 0.125D;
        updateDeliveryRate(now, bytes, applicationLimited || idleRestartSample);
        lastAckNanos = now;
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
        final boolean inflated = queueInflated();
        if (inflated && recentLoss >= 0.01D) {
            lossType = LossType.QUEUE;
            congestionReason = "RTT_INFLATION_LOSS";
            rttPressureActive = true;
            if (!congestionEpisodeActive && enterCongestion(now)) {
                congestionEpisodeActive = true;
            }
        } else if (inflated && burstDrainActive) {
            if (!rttPressureActive) enterRttPressure(now);
        } else if (recentLoss < 0.005D && lossQuiet(now)) {
            lossType = LossType.NONE;
            if (rttRecovered()) {
                rttPressureActive = false;
                congestionReason = "NONE";
            }
            congestionEpisodeActive = false;
        }
        updateCongestionModel(now, inFlightBytes, bytes);
        updateResumeValidation(now, bytes, packetSentNanos);
        publishMetrics();
    }

    void onLoss(int bytes, boolean timeout) {
        if (!config.isAdaptiveTransportEnabled()) return;
        final long now = System.nanoTime();
        lossWindow.recordLoss(now);
        consecutiveLosses++;
        lastLossNanos = now;
        if (timeout && pathMtu.canDetectBlackHole() && rttRecovered()
                && lossSampleCount() >= 64 && lossRatio() < 0.02D
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
        if (resumeState == ResumeState.UNVALIDATED) enterResumeSafeRetreat();
        final boolean severeLoss = lossType == LossType.QUEUE || lossType == LossType.BURST
                || lossType == LossType.RATE_LIMIT;
        final boolean newCongestionResponse;
        if (congestionEpisodeActive) {
            newCongestionResponse = false;
        } else if (severeLoss) {
            newCongestionResponse = enterCongestion(now);
            if (newCongestionResponse) congestionEpisodeActive = true;
        } else {
            newCongestionResponse = true;
        }
        final double reduction = lossType == LossType.RATE_LIMIT ? 0.70D
                : lossType == LossType.QUEUE || lossType == LossType.BURST ? 0.75D : 0.85D;
        // A NACK range and the following RTT-inflated ACKs can describe the same
        // congestion episode for several seconds. Apply one transport response
        // until clean ACK feedback closes the episode instead of repeatedly
        // multiplying the admission rate down to its absolute minimum.
        if (newCongestionResponse) {
            packetsPerSecond = applyBurstDrainFloor(clampPps(packetsPerSecond * reduction));
            lossPacingCeiling = Math.min(lossPacingCeiling, packetsPerSecond);
            lossRecoveryUpdatedNanos = now;
            reduceBurstAdmission(reduction);
        }
        publishMetrics();
    }

    void onEcnCe() {
        if (!config.isAdaptiveTransportEnabled()) return;
        final long now = System.nanoTime();
        lossWindow.recordEcn(now);
        if (ecnCeRatio() >= 0.10D) {
            lossType = LossType.QUEUE;
            congestionReason = "ECN_CE";
            lastLossNanos = now;
            if (resumeState == ResumeState.UNVALIDATED) enterResumeSafeRetreat();
            if (!congestionEpisodeActive) {
                congestionEpisodeActive = true;
                congestionMode = CongestionMode.DRAIN;
                congestionWindowBytes = Math.max(minimumCongestionWindow(), congestionWindowBytes * 7L / 8L);
                packetsPerSecond = clampPps(packetsPerSecond * 0.90D);
                lossPacingCeiling = Math.min(lossPacingCeiling, packetsPerSecond);
                lossRecoveryUpdatedNanos = now;
                reduceBurstAdmission(0.90D);
            }
        }
        publishMetrics();
    }

    LossType lossType() { return lossType; }
    CongestionMode congestionMode() { return congestionMode; }
    long congestionWindowBytes() { return congestionWindowBytes; }
    double packetsPerSecond() { return packetsPerSecond; }
    long burstAdmissionRateBytesPerSecond() { return burstAdmissionRateBytesPerSecond; }
    long burstAdmissionTargetBytesPerSecond() { return burstAdmissionTargetBytesPerSecond; }
    long burstRecoveryProbes() { return burstRecoveryProbes; }
    long validatedPathRateBytesPerSecond() { return maxBandwidth(); }
    ResumeState resumeState() { return resumeState; }
    int resumeValidatedRounds() { return resumeValidatedRounds; }

    void onApplicationQueued(long now, int bytes) {
        if (!config.isAdaptiveTransportEnabled() || bytes <= 0) return;
        updateApplicationArrivalRate(now);
        applicationArrivalRateBytesPerSecond += bytes * 1_000_000_000D
                / APPLICATION_ARRIVAL_TIME_CONSTANT_NANOS;
    }

    boolean shouldUseFec() {
        final double ratio = lossRatio();
        return config.isAdaptiveTransportEnabled() && lossType == LossType.RANDOM
                && ratio >= 0.005D && ratio <= 0.03D
                && !burstDrainActive && !queueInflated();
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
        final long bdp = Math.max(minimumCongestionWindow(), multiply(bandwidth, rtt) / 1_000_000_000L);
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
            final long target = Math.min(maximumCongestionWindow(), add(bdp * 2L, ackAggregationBytes));
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
            if (outstandingBytes <= burstDrainExitBytes()) {
                burstDrainActive = false;
                burstDrainStartedNanos = 0;
                burstDrainFloorPps = 0D;
                stopBurstAdmission();
                return;
            }
        } else if (outstandingBytes >= burstDrainEnterBytes()) {
            burstDrainActive = true;
            burstDrainStartedNanos = now;
            if (calibrationStartedNanos == 0L) calibrationStartedNanos = now;
            startBurstAdmission(now);
        } else {
            return;
        }

        final double estimatedPayloadBytes = Math.max(512D, config.getMTU() * 0.75D);
        final double recentLoss = lossRatio();
        final boolean workConserving = workConservingBulk(now);
        final long drainHorizon = burstDrainHorizonNanos();
        double target = Math.max(100D,
                outstandingBytes / estimatedPayloadBytes * 1_000_000_000D / drainHorizon);
        if (rttPressureActive && !rttRecovered()) {
            // Do not let the backlog PPS floor immediately undo a delay-based
            // admission cut on the next sendBudget call. Capacity recovery stays
            // ACK-driven until smoothed RTT returns close to the path minimum.
            burstDrainFloorPps = 0D;
            updateBurstAdmissionTarget(now, outstandingBytes, estimatedPayloadBytes,
                    Math.max(config.getAdaptiveMinPps(), packetsPerSecond));
            return;
        }
        final boolean severe = lossType == LossType.RATE_LIMIT || lossType == LossType.QUEUE
                || lossType == LossType.MTU_BLACK_HOLE || recentLoss >= 0.03D;
        final double ceiling;
        if (workConserving) {
            // A healthy bulk sender is work-conserving: do not let a
            // sender-limited bandwidth estimate pin PPS to its own low sample.
            // cwnd remains authoritative and RTT pressure still exits above.
            target = Math.max(target, config.getAdaptiveMaxPps());
            ceiling = config.getAdaptiveMaxPps();
        } else if (severe) {
            if (!lossQuiet(now)) {
                // A large application backlog is not evidence of available network capacity.
                // Do not let multi-megabyte chunk queues override an active policer/queue signal.
                burstDrainFloorPps = 0D;
                return;
            }
            ceiling = Math.min(config.getAdaptiveMaxPps(), qosRecoveryCeiling(now));
        } else if (lossType != LossType.NONE || recentLoss >= 0.005D || !lossQuiet(now)) {
            // Isolated/reordered losses may coexist with useful delivery. Permit only a bounded
            // step above the current rate instead of jumping to the healthy burst ceiling.
            ceiling = Math.min(Math.min(DEGRADED_BURST_MAX_PPS, config.getAdaptiveMaxPps()),
                    Math.max(QOS_RECOVERY_BASE_PPS, packetsPerSecond * 1.25D));
        } else {
            // During the first minute the PPS model can still grow quickly, but
            // workConservingBulk() keeps the byte bucket authoritative. This
            // avoids the unpaced join burst that previously produced seconds
            // of fragment HOL before a useful path sample existed.
            ceiling = healthyBurstCeiling();
        }
        if (minRtt != Long.MAX_VALUE && smoothedRtt >= minRtt * 2L) target *= 0.85D;
        burstDrainFloorPps = Math.max(config.getAdaptiveMinPps(), Math.min(ceiling, target));
        updateBurstAdmissionTarget(now, outstandingBytes, estimatedPayloadBytes, ceiling);
        packetsPerSecond = applyBurstDrainFloor(packetsPerSecond);
        if (!Double.isInfinite(lossPacingCeiling)) {
            lossPacingCeiling = Math.max(lossPacingCeiling, burstDrainFloorPps);
        }
    }

    private double applyBurstDrainFloor(double rate) {
        return burstDrainActive ? Math.max(rate, burstDrainFloorPps) : rate;
    }

    private double qosRecoveryCeiling(long now) {
        final long quietThreshold = Math.max(TimeUnit.SECONDS.toNanos(1),
                multiply(smoothedRtt, 4L));
        final long recoveryNanos = Math.max(0L, now - lastLossNanos - quietThreshold);
        final long stepNanos = Math.max(MIN_QOS_RECOVERY_STEP_NANOS, smoothedRtt);
        final double steps = recoveryNanos / (double) stepNanos;
        final double base = Math.max(QOS_RECOVERY_BASE_PPS, config.getAdaptiveMinPps());
        return Math.min(DEGRADED_BURST_MAX_PPS, base * Math.pow(BURST_ADMISSION_GROWTH, steps));
    }

    private void startBurstAdmission(long now) {
        final long validatedRate = maxBandwidth();
        final long packetRate = (long) Math.ceil(packetsPerSecond * Math.max(256D, averagePacketBytes));
        final long minimum = burstAdmissionMinimumBytesPerSecond();
        long initial = Math.max(minimum,
                Math.min(BURST_ADMISSION_INITIAL_BPS, packetRate));
        final boolean healthy = lossType == LossType.NONE && lossRatio() < 0.005D
                && !queueInflated() && lossQuietForRecovery(now);
        final boolean validatedRateIsFresh = validatedRate > 0L && lastValidatedBandwidthNanos != 0L
                && now - lastValidatedBandwidthNanos <= VALIDATED_PATH_LIFETIME_NANOS;
        resumeStartedNanos = now;
        resumeValidationAckedBytes = 0L;
        resumeValidatedRounds = 0;
        unvalidatedBandwidthSample = 0L;
        if (validatedRateIsFresh && healthy) {
            // Same live path: BBR-style idle restart at the already validated
            // bottleneck rate. The one-datagram initial token prevents a burst.
            initial = Math.max(minimum, validatedRate);
            resumeState = ResumeState.VALIDATED;
        } else if (validatedRate > 0L && healthy) {
            // Stale retained state is useful but no longer fully trusted. Follow
            // Careful Resume: start at half rate and validate two packet rounds.
            initial = Math.max(minimum, validatedRate / 2L);
            resumeState = ResumeState.UNVALIDATED;
        } else if (healthy) {
            // A brand-new path has no retained capacity to justify bypassing
            // byte admission. Validate two small packet rounds at the bootstrap
            // rate, then let ACK-driven samples raise the target geometrically.
            resumeState = ResumeState.UNVALIDATED;
        } else {
            resumeState = ResumeState.SAFE_RETREAT;
        }
        final long configuredMaximum = (long) Math.ceil(config.getAdaptiveMaxPps()
                * Math.max(256D, averagePacketBytes));
        initial = Math.min(configuredMaximum, initial);
        final long validationRtt = minRtt == Long.MAX_VALUE
                ? Math.max(TimeUnit.MILLISECONDS.toNanos(20), smoothedRtt)
                : minRtt;
        resumeValidationRoundBytes = Math.max(minimumCongestionWindow(),
                multiply(initial, Math.max(1L, validationRtt)) / 1_000_000_000L);
        burstAdmissionRateBytesPerSecond = initial;
        burstAdmissionTargetBytesPerSecond = initial;
        burstAdmissionTokens = Math.max(1, config.getMTU());
        burstAdmissionUpdatedNanos = now;
        burstRampUpdatedNanos = now;
    }

    private void stopBurstAdmission() {
        burstAdmissionRateBytesPerSecond = 0L;
        burstAdmissionTargetBytesPerSecond = 0L;
        burstAdmissionTokens = 0D;
        burstAdmissionUpdatedNanos = 0L;
        burstRampUpdatedNanos = 0L;
        resumeState = ResumeState.IDLE;
    }

    private void updateBurstAdmissionTarget(long now, long outstandingBytes,
                                            double estimatedPayloadBytes, double pacingCeiling) {
        if (!burstDrainActive || burstAdmissionRateBytesPerSecond <= 0L) return;
        updateApplicationArrivalRate(now);
        final long validatedRate = maxBandwidth();
        final long configuredMaximum = (long) Math.ceil(config.getAdaptiveMaxPps()
                * Math.max(256D, averagePacketBytes));
        final long pacingMaximum = (long) Math.ceil(pacingCeiling
                * Math.max(256D, estimatedPayloadBytes));
        final long minimum = burstAdmissionMinimumBytesPerSecond();
        final long observedMaximum = resumeState == ResumeState.UNVALIDATED
                ? Math.max(minimum, burstAdmissionRateBytesPerSecond)
                : validatedRate > 0L
                ? Math.max(minimum, (long) Math.ceil(validatedRate * 1.25D))
                : (long) Math.ceil(Math.min(DEGRADED_BURST_MAX_PPS, config.getAdaptiveMaxPps())
                * Math.max(256D, estimatedPayloadBytes));
        final long safeMaximum = Math.max(minimum,
                Math.min(configuredMaximum, Math.min(pacingMaximum, observedMaximum)));
        final long arrivalTarget = (long) Math.ceil(applicationArrivalRateBytesPerSecond
                * APPLICATION_ARRIVAL_HEADROOM);
        final long drainTarget = multiply(Math.max(0L, outstandingBytes), 1_000_000_000L)
                / burstDrainHorizonNanos();
        final boolean trustedHealthyPath = resumeState == ResumeState.VALIDATED
                && validatedRate > 0L && lossType == LossType.NONE
                && lastValidatedBandwidthNanos != 0L
                && now - lastValidatedBandwidthNanos <= VALIDATED_PATH_LIFETIME_NANOS
                && lossRatio() < 0.005D && !queueInflated() && lossQuietForRecovery(now);
        final long capacityFloor = trustedHealthyPath
                ? (long) Math.ceil(validatedRate * VALIDATED_CAPACITY_FLOOR) : 0L;
        final long demandTarget = Math.max(minimum,
                Math.max(capacityFloor, Math.max(arrivalTarget, drainTarget)));
        burstAdmissionTargetBytesPerSecond = Math.min(safeMaximum, demandTarget);
    }

    private void updateApplicationArrivalRate(long now) {
        if (applicationArrivalRateUpdatedNanos == 0L) {
            applicationArrivalRateUpdatedNanos = now;
            return;
        }
        final long elapsed = Math.max(0L, now - applicationArrivalRateUpdatedNanos);
        if (elapsed > 0L) {
            applicationArrivalRateBytesPerSecond *= Math.exp(-elapsed
                    / (double) APPLICATION_ARRIVAL_TIME_CONSTANT_NANOS);
            applicationArrivalRateUpdatedNanos = now;
        }
    }

    private void updateBurstAdmission(long now, int datagramBytes) {
        if (!burstDrainActive || burstAdmissionRateBytesPerSecond <= 0L) return;
        if (burstAdmissionUpdatedNanos == 0L) burstAdmissionUpdatedNanos = now;
        final long elapsed = Math.max(0L, now - burstAdmissionUpdatedNanos);
        final double tokenLimit = (double) Math.max(datagramBytes, config.getMTU())
                * BURST_ADMISSION_TOKEN_DATAGRAMS;
        burstAdmissionTokens = Math.min(tokenLimit, burstAdmissionTokens
                + elapsed * burstAdmissionRateBytesPerSecond / 1_000_000_000D);
        burstAdmissionUpdatedNanos = now;

        final long rtt = Math.max(TimeUnit.MILLISECONDS.toNanos(20),
                smoothedRtt > 0 ? smoothedRtt : TimeUnit.MILLISECONDS.toNanos(100));
        final long rampElapsed = Math.max(0L, now - burstRampUpdatedNanos);
        if (rampElapsed < rtt) return;
        final long target = Math.max(burstAdmissionMinimumBytesPerSecond(),
                burstAdmissionTargetBytesPerSecond);
        final long growthInterval = Math.min(rampElapsed, BURST_ADMISSION_MAX_GROWTH_INTERVAL_NANOS);
        if (target < burstAdmissionRateBytesPerSecond) {
            final long updated = Math.max(target, (long) Math.floor(burstAdmissionRateBytesPerSecond
                    / Math.pow(2D, growthInterval / (double) TimeUnit.SECONDS.toNanos(1))));
            burstAdmissionRateBytesPerSecond = updated;
            burstRampUpdatedNanos = now;
            return;
        }

        final boolean recentAck = lastAckNanos != 0L && now - lastAckNanos <= Math.max(rtt * 2L,
                TimeUnit.MILLISECONDS.toNanos(250));
        final boolean healthy = lossRatio() < 0.005D && !queueInflated() && lossQuietForRecovery(now);
        if (!recentAck || !healthy || resumeState == ResumeState.UNVALIDATED) {
            // Do not bank rate-growth credit while ACK feedback is absent or the path is unhealthy.
            // Spending that credit on recovery turns a held queue into a one-sample drain spike.
            burstRampUpdatedNanos = now;
            return;
        }
        final double grown = burstAdmissionRateBytesPerSecond
                * Math.pow(2D, growthInterval / (double) TimeUnit.SECONDS.toNanos(1));
        final long updated = Math.min(target, Math.max(burstAdmissionRateBytesPerSecond + 1L,
                (long) Math.ceil(grown)));
        if (updated > burstAdmissionRateBytesPerSecond) {
            burstAdmissionRateBytesPerSecond = updated;
            burstRecoveryProbes++;
        }
        burstRampUpdatedNanos = now;
    }

    private void reduceBurstAdmission(double reduction) {
        final long now = System.nanoTime();
        final long minimum = burstAdmissionMinimumBytesPerSecond();
        if (burstDrainActive && burstAdmissionRateBytesPerSecond > 0L) {
            burstAdmissionRateBytesPerSecond = Math.max(minimum,
                    (long) Math.floor(burstAdmissionRateBytesPerSecond * reduction));
            burstAdmissionTargetBytesPerSecond = Math.min(burstAdmissionTargetBytesPerSecond,
                    burstAdmissionRateBytesPerSecond);
            burstAdmissionTokens = Math.min(burstAdmissionTokens, config.getMTU());
            burstRampUpdatedNanos = now;
        }
    }

    private boolean lossQuietForRecovery(long now) {
        return lastLossNanos == 0L || now - lastLossNanos
                > Math.max(TimeUnit.MILLISECONDS.toNanos(250), multiply(smoothedRtt, 2L));
    }

    private long burstAdmissionMinimumBytesPerSecond() {
        return Math.max(1L, (long) Math.ceil(config.getAdaptiveMinPps()
                * Math.max(256D, averagePacketBytes)));
    }

    private long burstDrainEnterBytes() {
        // A fixed 48KiB trigger self-locks after an application-limited sample:
        // cwnd and pacing shrink, a 20-40KiB queue can no longer drain, yet it
        // never becomes BULK. Scale the trigger down with the current cwnd while
        // retaining four datagrams as the minimum useful pressure signal.
        return Math.min(BURST_DRAIN_ENTER_BYTES,
                Math.max(minimumCongestionWindow(), congestionWindowBytes));
    }

    private long burstDrainExitBytes() {
        final long enter = burstDrainEnterBytes();
        return Math.min(BURST_DRAIN_EXIT_BYTES,
                Math.max(2L * config.getMTU(), enter / 3L));
    }

    private long burstDrainHorizonNanos() {
        final long referenceRtt = smoothedRtt > 0L ? smoothedRtt
                : minRtt != Long.MAX_VALUE ? minRtt : TimeUnit.MILLISECONDS.toNanos(100);
        return Math.max(MIN_BURST_DRAIN_HORIZON_NANOS,
                Math.min(MAX_BURST_DRAIN_HORIZON_NANOS,
                        multiply(referenceRtt, BURST_DRAIN_RTT_MULTIPLIER)));
    }

    private boolean calibrationWindowOpen(long now) {
        if (calibrationStartedNanos == 0L
                || now - calibrationStartedNanos >= STARTUP_CALIBRATION_NANOS) return false;
        return true;
    }

    private boolean calibrationProbeAllowed(double recentLoss) {
        return recentLoss < 0.03D && !queueInflated() && !rttPressureActive
                && lossType != LossType.QUEUE && lossType != LossType.RATE_LIMIT
                && lossType != LossType.BURST && lossType != LossType.MTU_BLACK_HOLE;
    }

    private boolean workConservingBulk(long now) {
        if (!burstDrainActive) return false;
        // Cold-start traffic stays byte-paced even when it looks healthy: the
        // absence of loss before the first bulk flight is not path validation.
        // Established paths may bypass the byte bucket only after the protected
        // calibration minute and after retained capacity has been validated.
        return !calibrationWindowOpen(now)
                && resumeState == ResumeState.VALIDATED
                && lastValidatedBandwidthNanos != 0L
                && now - lastValidatedBandwidthNanos <= VALIDATED_PATH_LIFETIME_NANOS
                && lossSampleCount() >= 64
                && calibrationProbeAllowed(lossRatio());
    }

    private void enterRttPressure(long now) {
        rttPressureActive = true;
        congestionEpisodeActive = true;
        congestionMode = CongestionMode.DRAIN;
        congestionReason = "RTT_INFLATION";
        lastLossNanos = now;
        bandwidthProbeSuppressedUntil = Math.max(bandwidthProbeSuppressedUntil,
                now + Math.max(TimeUnit.SECONDS.toNanos(1), multiply(smoothedRtt, 4L)));
        gainCycle = 2;
        congestionWindowBytes = Math.max(minimumCongestionWindow(), congestionWindowBytes * 7L / 8L);
        final double inflation = minRtt == Long.MAX_VALUE || minRtt <= 0L
                ? 1D : smoothedRtt / (double) minRtt;
        final double reduction = inflation >= 2D ? 0.75D : 0.85D;
        packetsPerSecond = clampPps(packetsPerSecond * reduction);
        burstDrainFloorPps = Math.min(burstDrainFloorPps, packetsPerSecond);
        lossPacingCeiling = Math.min(lossPacingCeiling, packetsPerSecond);
        lossRecoveryUpdatedNanos = now;
        reduceBurstAdmission(reduction);
    }

    private boolean rttRecovered() {
        return minRtt == Long.MAX_VALUE || smoothedRtt <= minRtt * 5L / 4L;
    }

    private double healthyBurstCeiling() {
        // A new connection has no evidence that the path can absorb a multi-megabyte join burst.
        // Keep the old ceiling until at least one useful ACK window has been observed, then permit
        // at most a 2x demand-driven step toward the configured healthy ceiling.
        if (lossSampleCount() < 64) {
            return Math.min(DEGRADED_BURST_MAX_PPS, config.getAdaptiveMaxPps());
        }
        return Math.min(config.getAdaptiveMaxPps(),
                Math.max(DEGRADED_BURST_MAX_PPS, packetsPerSecond * 2D));
    }

    private double applyPacingSlew(long now, double targetRate) {
        if (targetRate <= packetsPerSecond) {
            pacingRateUpdatedNanos = now;
            return targetRate;
        }
        final long elapsed = Math.min(TimeUnit.SECONDS.toNanos(1),
                Math.max(0L, now - pacingRateUpdatedNanos));
        pacingRateUpdatedNanos = now;
        if (!Double.isInfinite(lossPacingCeiling) && lossQuietForRecovery(now)) {
            final long recoveryStep = Math.max(MIN_QOS_RECOVERY_STEP_NANOS, smoothedRtt);
            final double steps = elapsed / (double) recoveryStep;
            return Math.min(targetRate, packetsPerSecond
                    * Math.pow(BURST_ADMISSION_GROWTH, Math.min(steps, 8D)));
        }
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
                        multiply(smoothedRtt, 8L)));
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
        final long quietPeriod = Math.max(TimeUnit.MILLISECONDS.toNanos(250),
                multiply(smoothedRtt, 2L));
        if (now - lastLossNanos > quietPeriod) {
            final long elapsed = Math.max(0L, now - lossRecoveryUpdatedNanos);
            final long recoveryStep = Math.max(MIN_QOS_RECOVERY_STEP_NANOS, smoothedRtt);
            final long steps = elapsed / recoveryStep;
            if (steps > 0) {
                // Probe upward once per RTT after a short quiet period. A repeated
                // loss immediately cuts the ceiling again, while a clean path can
                // recover useful Minecraft burst throughput in a few RTTs.
                lossPacingCeiling *= Math.pow(BURST_ADMISSION_GROWTH, Math.min(steps, 2L));
                // Recovery is ACK-driven: do not accumulate silent time and
                // spend it as a large rate jump on the first returning ACK.
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
                > Math.max(TimeUnit.SECONDS.toNanos(1), multiply(smoothedRtt, 4L));
    }

    private void updateMode(long now, long inFlightBytes) {
        final long rtt = minRtt == Long.MAX_VALUE ? Math.max(1, smoothedRtt) : minRtt;
        final long bdp = Math.max(minimumCongestionWindow(), multiply(maxBandwidth(), rtt) / 1_000_000_000L);
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
            if (gainCycle == 0 && bandwidthCycleHadValidSample) advanceBandwidthFilterCycle();
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
        final long expected = multiply(maxBandwidth(), now - ackEpochStart) / 1_000_000_000L;
        final long excess = Math.max(0, ackEpochBytes - expected);
        ackAggregationBytes = Math.min(minimumCongestionWindow() * 4L,
                Math.max(ackAggregationBytes * 7L / 8L, excess));
    }

    private void updateDeliveryRate(long now, int bytes, boolean applicationLimited) {
        if (deliverySampleStarted == 0) deliverySampleStarted = now;
        deliverySampleBytes += bytes;
        deliverySampleApplicationLimited |= applicationLimited;
        final long interval = now - deliverySampleStarted;
        final long samplingWindow = Math.max(TimeUnit.MILLISECONDS.toNanos(1),
                Math.min(TimeUnit.MILLISECONDS.toNanos(100), Math.max(1, smoothedRtt / 2L)));
        if (interval < samplingWindow) return;
        final long rawSample = multiply(deliverySampleBytes, 1_000_000_000L) / interval;
        // ACK compression can report a delivery rate higher than this sender
        // actually injected. BBR applies the same send-rate bound before a
        // sample is allowed to become retained path capacity.
        final long sendRateCeiling = burstDrainActive && burstAdmissionRateBytesPerSecond > 0L
                && !workConservingBulk(now)
                ? burstAdmissionRateBytesPerSecond
                : (long) Math.ceil(packetsPerSecond * Math.max(256D, averagePacketBytes));
        final long sample = Math.min(rawSample, Math.max(1L, sendRateCeiling));
        lastDeliverySampleApplicationLimited = deliverySampleApplicationLimited;
        if (resumeState == ResumeState.UNVALIDATED) {
            // Feedback from the tentative resume can be queue-inflated. Keep it
            // separate until the resumed rate survives two packet rounds.
            if (!deliverySampleApplicationLimited) {
                unvalidatedBandwidthSample = Math.max(unvalidatedBandwidthSample, sample);
            }
        } else if (!deliverySampleApplicationLimited || sample >= maxBandwidth()) {
            bandwidthFilter[bandwidthCycle] = Math.max(bandwidthFilter[bandwidthCycle], sample);
            bandwidthCycleHadValidSample |= !deliverySampleApplicationLimited;
            if (!deliverySampleApplicationLimited) lastValidatedBandwidthNanos = now;
        }
        deliveryRateBytesPerSecond = maxBandwidth();
        deliverySampleStarted = now;
        deliverySampleBytes = 0;
        deliverySampleApplicationLimited = false;
    }

    private void updateResumeValidation(long now, int acknowledgedBytes, long packetSentNanos) {
        if (resumeState != ResumeState.UNVALIDATED || packetSentNanos < resumeStartedNanos) return;
        resumeValidationAckedBytes = add(resumeValidationAckedBytes, acknowledgedBytes);
        final long roundBytes = Math.max(minimumCongestionWindow(), resumeValidationRoundBytes);
        while (resumeValidationAckedBytes >= roundBytes && resumeValidatedRounds < RESUME_VALIDATION_ROUNDS) {
            resumeValidationAckedBytes -= roundBytes;
            resumeValidatedRounds++;
        }
        if (resumeValidatedRounds < RESUME_VALIDATION_ROUNDS) return;
        resumeState = ResumeState.VALIDATED;
        if (unvalidatedBandwidthSample > 0L) {
            bandwidthFilter[bandwidthCycle] = Math.max(
                    bandwidthFilter[bandwidthCycle], unvalidatedBandwidthSample);
            bandwidthCycleHadValidSample = true;
        }
        lastValidatedBandwidthNanos = now;
        deliveryRateBytesPerSecond = maxBandwidth();
        unvalidatedBandwidthSample = 0L;
    }

    private void enterResumeSafeRetreat() {
        resumeState = ResumeState.SAFE_RETREAT;
        resumeValidationAckedBytes = 0L;
        resumeValidatedRounds = 0;
        unvalidatedBandwidthSample = 0L;
    }

    private void advanceBandwidthFilterCycle() {
        bandwidthCycle = (bandwidthCycle + 1) % bandwidthFilter.length;
        bandwidthFilter[bandwidthCycle] = 0L;
        bandwidthCycleHadValidSample = false;
    }

    private double lossRatio() {
        return lossWindow.lossRatio(System.nanoTime());
    }

    private double currentLossRatio() {
        return lossWindow.currentLossRatio();
    }

    private double ecnCeRatio() {
        return lossWindow.currentEcnRatio();
    }

    private int recordLargeLoss(long now) {
        if (lastLargeLossNanos == 0 || now - lastLargeLossNanos > TimeUnit.SECONDS.toNanos(10)) largeLosses = 0;
        lastLargeLossNanos = now;
        return ++largeLosses;
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
        return lossWindow.sampleCount();
    }
    private long minimumCongestionWindow() { return Math.max(4L * config.getMTU(), 4L * 576L); }
    private long maximumCongestionWindow() { return Math.max(minimumCongestionWindow(), config.getMaxQueuedBytes()); }

    private void publishMetrics() {
        final long now = System.nanoTime();
        final boolean calibrating = calibrationWindowOpen(now);
        // Metrics must be observational. workConservingBulk() rotates the loss
        // window, so derive the displayed state from the already-current buckets.
        final boolean workConserving = burstDrainActive && (calibrating
                || calibrationProbeAllowed(currentLossRatio()));
        config.getMetrics().adaptivePacingRate(packetsPerSecond);
        config.getMetrics().adaptiveBytePacingRate(burstDrainActive && !workConserving
                ? burstAdmissionRateBytesPerSecond : 0L);
        config.getMetrics().adaptiveDemand(!burstDrainActive, burstDrainActive ? "BULK" : "IDLE",
                burstDrainActive ? Math.max(0L, now - burstDrainStartedNanos) : 0L,
                burstRecoveryProbes);
        config.getMetrics().adaptivePathModel(maxBandwidth(), lastDeliverySampleApplicationLimited,
                calibrating ? "CALIBRATING" : workConserving
                ? "WORK_CONSERVING" : resumeState.name(), resumeValidatedRounds);
        config.getMetrics().adaptiveDeliveryRate(deliveryRateBytesPerSecond);
        final long acknowledgements = lossWindow.acknowledgedCount();
        final long losses = lossWindow.lostCount();
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
