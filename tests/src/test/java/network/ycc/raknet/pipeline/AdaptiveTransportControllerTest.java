package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultConfig;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AdaptiveTransportControllerTest {
    @Test
    public void defaultPpsCeilingAllowsHealthyMinecraftBursts() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final DefaultConfig config = new DefaultConfig(channel);
        Assertions.assertEquals(2000, config.getAdaptiveMaxPps());
        channel.finishAndReleaseAll();
    }

    @Test
    public void startupDataTimeoutsCannotMasqueradeAsMtuBlackHole() {
        final RakNet.Config config = mock(RakNet.Config.class);
        when(config.isAdaptiveTransportEnabled()).thenReturn(true);
        when(config.getMTU()).thenReturn(1400);
        when(config.getAdaptiveMinPps()).thenReturn(50);
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        when(config.getPlpmtudMaxMtu()).thenReturn(1500);
        when(config.getMaxQueuedBytes()).thenReturn(3 * 1024 * 1024);
        when(config.getMetrics()).thenReturn(mock(RakNet.MetricsLogger.class));
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);

        controller.onLoss(1400, true);
        controller.onAck(128, 20_000_000L);
        controller.onLoss(1400, true);
        controller.onAck(128, 20_000_000L);
        controller.onLoss(1400, true);
        controller.applyPendingMtu();
        verify(config, never()).setMTU(1200);
        Assertions.assertNotEquals(AdaptiveTransportController.LossType.MTU_BLACK_HOLE,
                controller.lossType());
    }

    @Test
    public void completedPathCanStillDetectConfirmedDataBlackHole() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getPlpmtudMaxMtu()).thenReturn(1400);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        for (int i = 0; i < 200; i++) controller.onAck(128, 20_000_000L, 0);

        for (int i = 0; i < 3; i++) {
            controller.onLoss(1400, true);
            controller.onAck(128, 20_000_000L, 0);
        }
        controller.applyPendingMtu();

        Assertions.assertEquals(AdaptiveTransportController.LossType.MTU_BLACK_HOLE,
                controller.lossType());
        verify(config).setMTU(1200);
    }

    @Test
    public void pacingNeverReturnsNegativeDelay() {
        final RakNet.Config config = mock(RakNet.Config.class);
        when(config.isAdaptiveTransportEnabled()).thenReturn(true);
        when(config.getMTU()).thenReturn(1400);
        when(config.getAdaptiveMinPps()).thenReturn(50);
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        when(config.getPlpmtudMaxMtu()).thenReturn(1500);
        when(config.getMaxQueuedBytes()).thenReturn(3 * 1024 * 1024);
        when(config.getMetrics()).thenReturn(mock(RakNet.MetricsLogger.class));
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        final long now = System.nanoTime();
        Assertions.assertEquals(1, controller.sendBudget(now));
        Assertions.assertEquals(1, controller.sendBudget(now));
        Assertions.assertEquals(0, controller.sendBudget(now));
        Assertions.assertEquals(1, controller.sendBudget(now + 2_000_000L));
        Assertions.assertTrue(controller.nanosUntilSend(now) >= 0);
    }

    @Test
    public void healthyBacklogCalibrationUsesBoundedPpsGrowth() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        for (int i = 0; i < 64; i++) controller.onAck(1200, 40_000_000L, 0);
        setDouble(controller, "packetsPerSecond", 400D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 400 * 1024);

        Assertions.assertTrue(controller.packetsPerSecond() > 400D);
        Assertions.assertTrue(controller.packetsPerSecond() <= 800D,
                "calibration must not jump directly to the configured ceiling");
    }

    @Test
    public void newConnectionCalibrationUsesConservativePpsCeiling() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        setDouble(controller, "packetsPerSecond", 50D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 4 * 1024 * 1024);

        Assertions.assertEquals(600D, controller.packetsPerSecond(), 0.01D);
        Assertions.assertEquals(AdaptiveTransportController.ResumeState.UNVALIDATED,
                controller.resumeState());
    }

    @Test
    public void newBacklogCalibrationUsesPpsAndByteBurstLimits() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();

        Assertions.assertEquals(1, controller.sendBudget(now, 0, 1400, 1024 * 1024));
        Assertions.assertEquals(0, controller.sendBudget(now, 0, 1400, 1024 * 1024));
        Assertions.assertEquals(384L * 1024L, controller.burstAdmissionRateBytesPerSecond());
        Assertions.assertEquals(AdaptiveTransportController.ResumeState.UNVALIDATED,
                controller.resumeState());
        setDouble(controller, "pacingTokens", 1D);
        setLong(controller, "tokenUpdatedNanos", now);
        setDouble(controller, "burstAdmissionTokens", 0D);
        setLong(controller, "burstAdmissionUpdatedNanos", now);
        Assertions.assertEquals(0, controller.sendBudget(now, 0, 1400, 1024 * 1024),
                "cold start must not bypass byte admission before path validation");
        final int budget = controller.sendBudget(now + 10_000_000L, 0, 1400, 1024 * 1024);
        Assertions.assertTrue(budget > 0 && budget <= 4,
                "calibration may replenish byte tokens but must keep the PPS burst bound");
    }

    @Test
    public void calibrationPublishesItsActiveByteAdmissionLimit()
            throws Exception {
        final RakNet.Config config = adaptiveConfig();
        final RakNet.MetricsLogger metrics = config.getMetrics();
        final AdaptiveTransportController controller =
                new AdaptiveTransportController(config);
        setDouble(controller, "packetsPerSecond", 30D);

        controller.sendBudget(
                System.nanoTime(),
                0,
                1400,
                1024 * 1024
        );

        verify(metrics).adaptiveBytePacingRate(128L * 1024L);
        verify(metrics).adaptiveAdmissionDiagnostics(
                128L * 1024L,
                600D,
                false
        );
    }

    @Test
    public void completedBurstDoesNotRemainReportedAsCalibrating() {
        final RakNet.Config config = adaptiveConfig();
        final RakNet.MetricsLogger metrics = config.getMetrics();
        final AdaptiveTransportController controller =
                new AdaptiveTransportController(config);
        final long now = System.nanoTime();

        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        verify(metrics).adaptivePathModel(
                0L,
                false,
                "CALIBRATING",
                0
        );
        clearInvocations(metrics);

        controller.sendBudget(now + 1_000_000L, 0, 1400, 0);
        verify(metrics).adaptivePathModel(0L, false, "IDLE", 0);
    }

    @Test
    public void protectedCalibrationOnlyBypassesByteAdmissionAfterValidation() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        for (int i = 0; i < 100; i++) controller.onAck(600, 20_000_000L, 0);
        final long now = System.nanoTime();
        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        setDouble(controller, "pacingTokens", 1D);
        setLong(controller, "tokenUpdatedNanos", now);
        setDouble(controller, "burstAdmissionTokens", 0D);
        setLong(controller, "burstAdmissionUpdatedNanos", now);

        Assertions.assertEquals(0, controller.sendBudget(now, 0, 1400, 1024 * 1024),
                "healthy-looking cold start must remain byte paced");

        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1024L * 1024L);
        setObject(controller, "resumeState", AdaptiveTransportController.ResumeState.VALIDATED);
        setLong(controller, "lastValidatedBandwidthNanos", now);
        setLong(controller, "calibrationStartedNanos", now - 61_000_000_000L);
        setDouble(controller, "pacingTokens", 1D);
        setLong(controller, "tokenUpdatedNanos", now);
        setDouble(controller, "burstAdmissionTokens", 0D);
        setLong(controller, "burstAdmissionUpdatedNanos", now);
        Assertions.assertEquals(1, controller.sendBudget(now, 0, 1400, 1024 * 1024),
                "validated healthy bulk may become work-conserving after calibration");

        final long congested = now + 1;
        setObject(controller, "lossType", AdaptiveTransportController.LossType.RATE_LIMIT);
        setLong(controller, "lastLossNanos", congested);
        setDouble(controller, "pacingTokens", 1D);
        setLong(controller, "tokenUpdatedNanos", congested);
        setDouble(controller, "burstAdmissionTokens", 0D);
        setLong(controller, "burstAdmissionUpdatedNanos", congested);
        Assertions.assertEquals(0, controller.sendBudget(congested, 0, 1400, 1024 * 1024),
                "explicit congestion must restore byte admission immediately");
    }

    @Test
    public void calibrationUsesBoundedDemandInsteadOfConfiguredMaximum() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        setDouble(controller, "packetsPerSecond", 30D);
        controller.sendBudget(now, 0, 1400, 1024 * 1024);

        controller.sendBudget(now + 10_000_000_000L, 0, 1400, 1024 * 1024);

        Assertions.assertEquals(600D, controller.packetsPerSecond(), 0.01D,
                "unvalidated cold start must retain the bounded probe ceiling");
    }

    @Test
    public void unknownBacklogEscapesSenderLimitedThirtyPpsSample()
            throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        setDouble(controller, "packetsPerSecond", 30D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 1024 * 1024);

        Assertions.assertEquals(
                128L * 1024L,
                controller.burstAdmissionRateBytesPerSecond(),
                "an unknown healthy path needs a bounded bootstrap independent "
                        + "of its sender-limited pacing sample"
        );
    }

    @Test
    public void lowFlightRttJitterDoesNotSuppressUnknownPathBootstrap()
            throws Exception {
        final AdaptiveTransportController controller =
                new AdaptiveTransportController(adaptiveConfig());
        setDouble(controller, "packetsPerSecond", 30D);
        setLong(controller, "minRtt", 14_000_000L);
        setLong(controller, "smoothedRtt", 25_000_000L);

        controller.sendBudget(
                System.nanoTime(),
                0,
                1400,
                512 * 1024
        );

        Assertions.assertEquals(
                128L * 1024L,
                controller.burstAdmissionRateBytesPerSecond()
        );
        Assertions.assertEquals(
                AdaptiveTransportController.ResumeState.UNVALIDATED,
                controller.resumeState()
        );
        Assertions.assertFalse(controller.rttPressureActive());
    }

    @Test
    public void validatedAdmissionCanProbeThroughLowFlightRttJitter()
            throws Exception {
        final AdaptiveTransportController controller =
                new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        setDouble(controller, "packetsPerSecond", 30D);
        setLong(controller, "minRtt", 14_000_000L);
        setLong(controller, "smoothedRtt", 25_000_000L);
        controller.sendBudget(now, 0, 1400, 512 * 1024);

        final Field bandwidth = AdaptiveTransportController.class
                .getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 128L * 1024L);
        setObject(
                controller,
                "resumeState",
                AdaptiveTransportController.ResumeState.VALIDATED
        );
        setLong(controller, "lastValidatedBandwidthNanos", now);
        setLong(controller, "lastAckNanos", now);
        setLong(controller, "burstRampUpdatedNanos", now - 250_000_000L);

        controller.sendBudget(
                now + 1_000_000L,
                0,
                1400,
                512 * 1024
        );

        Assertions.assertTrue(
                controller.burstAdmissionRateBytesPerSecond()
                        > 128L * 1024L
        );
        Assertions.assertFalse(controller.rttPressureActive());
    }

    @Test
    public void cleanAcksSlewBurstAdmissionWithoutCatchUpJump() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 2L * 1024L * 1024L);
        setLong(controller, "smoothedRtt", 40_000_000L);
        setLong(controller, "lastAckNanos", now);
        setLong(controller, "burstRampUpdatedNanos", now - 160_000_000L);
        setObject(controller, "resumeState", AdaptiveTransportController.ResumeState.VALIDATED);
        setLong(controller, "lastValidatedBandwidthNanos", now);

        controller.sendBudget(now, 0, 1400, 1024 * 1024);

        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() > 384L * 1024L);
        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() < 512L * 1024L);
        Assertions.assertEquals(1L, controller.burstRecoveryProbes());
    }

    @Test
    public void unhealthyBurstCannotBankGrowthForRecoverySpike() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 2L * 1024L * 1024L);
        setLong(controller, "smoothedRtt", 40_000_000L);
        setLong(controller, "lastAckNanos", now);
        setLong(controller, "lastLossNanos", now);
        setObject(controller, "lossType", AdaptiveTransportController.LossType.QUEUE);
        setLong(controller, "burstRampUpdatedNanos", now - 2_000_000_000L);

        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        Assertions.assertEquals(384L * 1024L, controller.burstAdmissionRateBytesPerSecond());

        setLong(controller, "lastLossNanos", 0L);
        setLong(controller, "lastAckNanos", now + 40_000_000L);
        setObject(controller, "lossType", AdaptiveTransportController.LossType.NONE);
        controller.sendBudget(now + 40_000_000L, 0, 1400, 1024 * 1024);

        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() < 512L * 1024L,
                "recovery must not spend the unhealthy interval as an immediate rate jump");
    }

    @Test
    public void shortIdleResumesAtValidatedPathRate() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1024L * 1024L);
        setLong(controller, "lastValidatedBandwidthNanos", now);

        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        controller.sendBudget(now + 10_000_000L, 0, 1400, 0);
        controller.sendBudget(now + 1_000_000_000L, 0, 1400, 1024 * 1024);

        Assertions.assertEquals(1024L * 1024L, controller.burstAdmissionRateBytesPerSecond());
        Assertions.assertEquals(AdaptiveTransportController.ResumeState.VALIDATED, controller.resumeState());
    }

    @Test
    public void lowCapacityPathDoesNotResumeAtBootstrapRate() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 64L * 1024L);
        setLong(controller, "lastValidatedBandwidthNanos", now);

        controller.sendBudget(now, 0, 1400, 1024 * 1024);

        Assertions.assertEquals(64L * 1024L, controller.burstAdmissionRateBytesPerSecond());
        Assertions.assertTrue(
                controller.burstAdmissionTargetBytesPerSecond()
                        <= 96L * 1024L
        );
        Assertions.assertEquals(AdaptiveTransportController.ResumeState.VALIDATED, controller.resumeState());
    }

    @Test
    public void establishedBacklogResumesAtValidatedRateThenTracksDemand() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        for (int i = 0; i < 64; i++) controller.onAck(1200, 40_000_000L, 0);
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 2L * 1024L * 1024L);
        setLong(controller, "lastValidatedBandwidthNanos", System.nanoTime());
        setDouble(controller, "packetsPerSecond", 2000D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 400 * 1024);

        Assertions.assertEquals(2L * 1024L * 1024L, controller.burstAdmissionRateBytesPerSecond());
        Assertions.assertEquals(AdaptiveTransportController.ResumeState.VALIDATED, controller.resumeState());
        Assertions.assertTrue(controller.burstAdmissionTargetBytesPerSecond() >= 1600L * 1024L,
                "actual=" + controller.burstAdmissionTargetBytesPerSecond());
        Assertions.assertTrue(controller.burstAdmissionTargetBytesPerSecond() <= 2200L * 1024L);
    }

    @Test
    public void largeApplicationBurstUsesSmoothedArrivalTarget() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        for (int i = 0; i < 64; i++) controller.onAck(1200, 40_000_000L, 0);
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 4L * 1024L * 1024L);
        setLong(controller, "lastValidatedBandwidthNanos", System.nanoTime());
        setDouble(controller, "packetsPerSecond", 1500D);
        final long now = System.nanoTime();

        controller.onApplicationQueued(now, 3 * 1024 * 1024);
        controller.sendBudget(now, 0, 1400, 3 * 1024 * 1024);

        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() >= 2200L * 1024L);
        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() <= 2400L * 1024L);
        Assertions.assertTrue(controller.burstAdmissionTargetBytesPerSecond() >= 2_000_000L,
                "actual=" + controller.burstAdmissionTargetBytesPerSecond());
        Assertions.assertTrue(controller.burstAdmissionTargetBytesPerSecond() <= 2_200_000L);
    }

    @Test
    public void healthyValidatedBurstRetainsMostOfKnownPathCapacity() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(3000);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        for (int i = 0; i < 64; i++) controller.onAck(1200, 40_000_000L, 0);
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 3L * 1024L * 1024L);
        final long now = System.nanoTime();
        setLong(controller, "lastValidatedBandwidthNanos", now);
        setLong(controller, "smoothedRtt", 40_000_000L);
        setLong(controller, "minRtt", 40_000_000L);
        setDouble(controller, "packetsPerSecond", 2500D);

        controller.sendBudget(now, 0, 1400, 252 * 1024);

        Assertions.assertEquals(AdaptiveTransportController.ResumeState.VALIDATED,
                controller.resumeState());
        Assertions.assertTrue(controller.burstAdmissionTargetBytesPerSecond() >= 2400L * 1024L,
                "healthy bulk must not fall back to the old 0.3-0.6MiB/s admission range; actual="
                        + controller.burstAdmissionTargetBytesPerSecond());
        Assertions.assertTrue(controller.burstAdmissionTargetBytesPerSecond() <= 3L * 1024L * 1024L);
    }

    @Test
    public void wallClockIdleDoesNotExpireValidatedBandwidth() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1024L * 1024L);
        final Field lossWindow = AdaptiveTransportController.class.getDeclaredField("lossWindow");
        lossWindow.setAccessible(true);
        setLong(lossWindow.get(controller), "bucketStarted", System.nanoTime() - 30_000_000_000L);

        controller.onAck(1200, 40_000_000L, 0, true, Long.MIN_VALUE);

        Assertions.assertEquals(1024L * 1024L, controller.validatedPathRateBytesPerSecond());
    }

    @Test
    public void applicationLimitedLowSampleCannotReduceValidatedBandwidth() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1024L * 1024L);
        setLong(controller, "deliverySampleStarted", System.nanoTime() - 100_000_000L);

        controller.onAck(1200, 40_000_000L, 0, true, Long.MIN_VALUE);

        Assertions.assertEquals(1024L * 1024L, controller.validatedPathRateBytesPerSecond());
    }

    @Test
    public void applicationLimitedHighSampleCannotInflateValidatedBandwidth() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 256L * 1024L);
        setDouble(controller, "packetsPerSecond", 3000D);
        setDouble(controller, "averagePacketBytes", 1400D);
        setLong(controller, "deliverySampleStarted", System.nanoTime() - 100_000_000L);
        setLong(controller, "deliverySampleBytes", 400L * 1024L);

        controller.onAck(1200, 20_000_000L, 0, true, Long.MIN_VALUE);

        Assertions.assertEquals(256L * 1024L,
                controller.validatedPathRateBytesPerSecond(),
                "an ACK-compressed application-limited sample must not become retained capacity");
    }

    @Test
    public void backloggedHighSampleCanRaiseValidatedBandwidth() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 256L * 1024L);
        setDouble(controller, "packetsPerSecond", 3000D);
        setDouble(controller, "averagePacketBytes", 1400D);
        setLong(controller, "deliverySampleStarted", System.nanoTime() - 100_000_000L);
        setLong(controller, "deliverySampleBytes", 400L * 1024L);

        controller.onAck(1200, 20_000_000L, 8 * 1024L,
                false, Long.MIN_VALUE);

        Assertions.assertTrue(controller.validatedPathRateBytesPerSecond()
                        > 3L * 1024L * 1024L,
                "a non-application-limited sample must still teach useful path capacity");
    }

    @Test
    public void intermittentApplicationTrafficRetainsValidatedBandwidthAcrossProbeCycles() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1024L * 1024L);
        final Method advance = AdaptiveTransportController.class
                .getDeclaredMethod("advanceBandwidthFilterCycle");
        advance.setAccessible(true);

        for (int i = 0; i < 8; i++) advance.invoke(controller);

        Assertions.assertEquals(1024L * 1024L, controller.validatedPathRateBytesPerSecond(),
                "short application-limited gaps must not erase a recently demonstrated path rate");
    }

    @Test
    public void collapsedCwndLetsSubFortyEightKibQueueEnterBulk() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        setLong(controller, "congestionWindowBytes", 12L * 1024L);
        setDouble(controller, "packetsPerSecond", 30D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 32 * 1024);

        Assertions.assertTrue(controller.packetsPerSecond() > 30D,
                "a queue larger than the collapsed cwnd must escape the IDLE pacing self-lock");
        Assertions.assertTrue(controller.packetsPerSecond() <= 600D,
                "an unvalidated path must retain the conservative probe ceiling");
        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() > 0L);
    }

    @Test
    public void stalePathStateResumesAtHalfRateAndValidatesTwoRounds() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1024L * 1024L);
        final long now = System.nanoTime();
        setLong(controller, "lastValidatedBandwidthNanos", now - 360_000_000_000L);
        setLong(controller, "smoothedRtt", 20_000_000L);
        setLong(controller, "minRtt", 20_000_000L);

        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        Assertions.assertEquals(512L * 1024L, controller.burstAdmissionRateBytesPerSecond());
        Assertions.assertEquals(AdaptiveTransportController.ResumeState.UNVALIDATED, controller.resumeState());

        final long roundBytes = 11L * 1024L;
        controller.onAck((int) roundBytes, 20_000_000L, 0, false, now);
        Assertions.assertEquals(1, controller.resumeValidatedRounds());
        controller.onAck((int) roundBytes, 20_000_000L, 0, false, now);

        Assertions.assertEquals(AdaptiveTransportController.ResumeState.VALIDATED, controller.resumeState());
        Assertions.assertEquals(2, controller.resumeValidatedRounds());
    }

    @Test
    public void congestionDuringUnvalidatedResumeEntersSafeRetreat() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1024L * 1024L);
        final long now = System.nanoTime();
        setLong(controller, "lastValidatedBandwidthNanos", now - 360_000_000_000L);

        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        controller.onLoss(1200, false);

        Assertions.assertEquals(AdaptiveTransportController.ResumeState.SAFE_RETREAT, controller.resumeState());
    }

    @Test
    public void healthyWorkConservingQueueKeepsBoundedAdmissionTarget() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        for (int i = 0; i < 64; i++) controller.onAck(1200, 40_000_000L, 0);
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1024L * 1024L);
        final long now = System.nanoTime();
        setLong(controller, "lastValidatedBandwidthNanos", now);
        setDouble(controller, "packetsPerSecond", 1000D);

        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        setLong(controller, "burstDrainStartedNanos", now - 3_000_000_000L);
        setLong(controller, "calibrationStartedNanos", now - 61_000_000_000L);
        controller.sendBudget(now + 1_000_000L, 0, 1400, 1024 * 1024);

        Assertions.assertEquals(1536L * 1024L,
                controller.burstAdmissionTargetBytesPerSecond(),
                "the bounded probe may use at most 1.5x validated capacity");
        Assertions.assertTrue(controller.burstAdmissionTargetBytesPerSecond() <= 1536L * 1024L,
                "work-conserving mode must not discard the validated-capacity ceiling");
    }

    @Test
    public void lowFlightRttInflationDoesNotCollapseHealthyBacklog()
            throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        setLong(controller, "burstAdmissionRateBytesPerSecond", 120L * 1024L);
        setLong(controller, "burstAdmissionTargetBytesPerSecond", 120L * 1024L);
        setLong(controller, "minRtt", 20_000_000L);
        setLong(controller, "minRttStamp", now);
        setLong(controller, "smoothedRtt", 20_000_000L);
        setDouble(controller, "packetsPerSecond", 100D);

        controller.onAck(1200, 200_000_000L, 0);

        Assertions.assertEquals(
                120L * 1024L,
                controller.burstAdmissionRateBytesPerSecond()
        );
        Assertions.assertFalse(controller.rttPressureActive());
        Assertions.assertTrue(controller.packetsPerSecond() >= 100D);
    }

    @Test
    public void highFlightRttInflationStillCutsAdmission() throws Exception {
        final AdaptiveTransportController controller =
                new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        setLong(
                controller,
                "burstAdmissionRateBytesPerSecond",
                120L * 1024L
        );
        setLong(
                controller,
                "burstAdmissionTargetBytesPerSecond",
                120L * 1024L
        );
        setLong(controller, "minRtt", 20_000_000L);
        setLong(controller, "minRttStamp", now);
        setLong(controller, "smoothedRtt", 20_000_000L);
        setDouble(controller, "packetsPerSecond", 100D);
        final Field bandwidth = AdaptiveTransportController.class
                .getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1024L * 1024L);

        controller.onAck(1200, 200_000_000L, 8 * 1024L);

        final double reducedPps = controller.packetsPerSecond();
        Assertions.assertTrue(controller.rttPressureActive());
        Assertions.assertTrue(
                controller.burstAdmissionRateBytesPerSecond()
                        < 120L * 1024L
        );
        Assertions.assertTrue(reducedPps < 100D);
        controller.sendBudget(now + 1_000_000L, 0, 1400, 1024 * 1024);
        Assertions.assertTrue(controller.packetsPerSecond() <= reducedPps,
                "the backlog floor must not undo an RTT-pressure cut on the next send");
    }

    @Test
    public void lowFlightAcksReleaseStaleRttPressure() throws Exception {
        final AdaptiveTransportController controller =
                new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        setLong(controller, "minRtt", 20_000_000L);
        setLong(controller, "minRttStamp", now);
        setLong(controller, "smoothedRtt", 20_000_000L);
        setDouble(controller, "packetsPerSecond", 100D);

        controller.onAck(1200, 200_000_000L, 8 * 1024L);
        Assertions.assertTrue(controller.rttPressureActive());
        for (int i = 0; i < 3; i++) {
            controller.onAck(1200, 200_000_000L, 0);
        }

        Assertions.assertFalse(controller.rttPressureActive());
    }

    @Test
    public void rollingQueueLossDoesNotHoldRecoveredRttPressure() throws Exception {
        final AdaptiveTransportController controller =
                new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        setLong(controller, "minRtt", 20_000_000L);
        setLong(controller, "minRttStamp", now);
        setLong(controller, "smoothedRtt", 40_000_000L);

        controller.onLoss(1200, false);
        controller.onAck(1200, 40_000_000L, 8 * 1024L);
        Assertions.assertEquals(AdaptiveTransportController.LossType.QUEUE,
                controller.lossType());
        Assertions.assertTrue(controller.rttPressureActive());

        // Preserve the rolling loss sample but make the individual loss event
        // old enough for ACK-driven delay recovery.
        setLong(controller, "lastLossNanos", 0L);
        for (int i = 0; i < 3; i++) {
            controller.onAck(1200, 20_000_000L, 0);
        }

        Assertions.assertEquals(AdaptiveTransportController.LossType.QUEUE,
                controller.lossType(),
                "the loss ceiling may remain while delay pressure recovers");
        Assertions.assertFalse(controller.rttPressureActive(),
                "the ten-second rolling loss window must not hold recovered delay pressure");
    }

    @Test
    public void confirmedQueueLossCanReduceAdmissionBelowOldFixedFloor() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        controller.sendBudget(System.nanoTime(), 0, 1400, 1024 * 1024);
        setLong(controller, "burstAdmissionRateBytesPerSecond", 140L * 1024L);
        setLong(controller, "burstAdmissionTargetBytesPerSecond", 140L * 1024L);
        setLong(controller, "minRtt", 20_000_000L);
        setLong(controller, "smoothedRtt", 40_000_000L);

        controller.onLoss(1200, false);

        Assertions.assertEquals(AdaptiveTransportController.LossType.QUEUE, controller.lossType());
        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() < 128L * 1024L);
    }

    @Test
    public void timeoutOnlyRttInflationDoesNotClaimForwardQueueLoss() throws Exception {
        final AdaptiveTransportController controller =
                new AdaptiveTransportController(adaptiveConfig());
        controller.sendBudget(System.nanoTime(), 0, 1400, 1024 * 1024);
        setLong(controller, "burstAdmissionRateBytesPerSecond", 442L * 1024L);
        setLong(controller, "burstAdmissionTargetBytesPerSecond", 442L * 1024L);
        setLong(controller, "minRtt", 20_000_000L);
        setLong(controller, "smoothedRtt", 70_000_000L);

        for (int i = 0; i < 3; i++) {
            controller.onLoss(1400, true);
        }
        controller.onAck(1400, 70_000_000L, 8 * 1024L);

        Assertions.assertEquals(AdaptiveTransportController.LossType.BURST,
                controller.lossType());
        Assertions.assertFalse(controller.rttPressureActive(),
                "an ambiguous timeout/ACK-loss episode must not become sticky queue pressure");
        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond()
                        > 128L * 1024L,
                "timeout loss still backs off, but must not collapse the byte gate");
    }

    @Test
    public void scheduledPacingReportsWakeupLatenessAndActualBatch() {
        final RakNet.Config config = adaptiveConfig();
        final RakNet.MetricsLogger metrics = config.getMetrics();
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        final long now = System.nanoTime();
        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        final long delay = controller.nanosUntilSend(now);

        controller.sendBudget(now + delay + 5_000_000L, 0, 1400, 1024 * 1024);
        controller.onPacingBatchSent(3);

        verify(metrics).pacingScheduler(5_000_000L, 3);
    }

    @Test
    public void persistentQueueLossReducesBurstAdmissionOncePerEpisode() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        setLong(controller, "burstAdmissionRateBytesPerSecond", 1024L * 1024L);
        setLong(controller, "burstAdmissionTargetBytesPerSecond", 1024L * 1024L);
        controller.onAck(1200, 20_000_000L, 0);
        controller.onAck(1200, 200_000_000L, 0);

        for (int i = 0; i < 8; i++) {
            setLong(controller, "lastCongestionResponseNanos", 0L);
            controller.onLoss(1200, false);
        }

        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() >= 700L * 1024L);
        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() <= 800L * 1024L);
    }

    @Test
    public void lossImmediatelyCutsActiveBurstAdmission() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        controller.sendBudget(System.nanoTime(), 0, 1400, 1024 * 1024);
        setLong(controller, "burstAdmissionRateBytesPerSecond", 1024L * 1024L);

        controller.onLoss(1200, false);

        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() < 1024L * 1024L);
        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() >= 128L * 1024L);
    }

    @Test
    public void tinyQueueCannotKeepBurstDrainActive() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(600);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        setDouble(controller, "packetsPerSecond", 50D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 400 * 1024);
        setDouble(controller, "packetsPerSecond", 50D);
        controller.sendBudget(System.nanoTime(), 0, 1400, 33);

        Assertions.assertEquals(50D, controller.packetsPerSecond(), 0.01D);
    }

    @Test
    public void zstdSizedBurstRemainsProtectedAfterMovingInFlight() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(600);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        setDouble(controller, "packetsPerSecond", 50D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 64 * 1024);
        Assertions.assertTrue(controller.packetsPerSecond() >= 100D);

        setDouble(controller, "packetsPerSecond", 50D);
        controller.sendBudget(System.nanoTime(), 64 * 1024, 1400, 0);
        Assertions.assertTrue(controller.packetsPerSecond() >= 100D);
    }

    @Test
    public void observedLargeZstdBatchUsesRttDrainTarget() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(600);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        setDouble(controller, "packetsPerSecond", 50D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 211 * 1024);

        Assertions.assertTrue(controller.packetsPerSecond() >= 500D);
        Assertions.assertTrue(controller.packetsPerSecond() <= 525D,
                "the initial queue should target the bounded four-RTT drain horizon");
    }

    @Test
    public void activeSevereLossDisablesBacklogDrainFloor() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(600);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        for (int i = 0; i < 9; i++) controller.onAck(1200, 40_000_000L, 0);
        controller.onLoss(1200, false);
        setDouble(controller, "packetsPerSecond", 50D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 211 * 1024);

        Assertions.assertEquals(50D, controller.packetsPerSecond(), 0.01D);
        Assertions.assertTrue(
                controller.burstAdmissionRateBytesPerSecond()
                        < 128L * 1024L,
                "congestion evidence must suppress the unknown-path bootstrap"
        );
    }

    @Test
    public void quietRateLimitRecoveryRampsInsteadOfJumpingToHealthyCeiling() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        setDouble(controller, "packetsPerSecond", 30D);
        setLong(controller, "lastLossNanos", System.nanoTime() - 1_600_000_000L);
        setObject(controller, "lossType", AdaptiveTransportController.LossType.RATE_LIMIT);

        controller.sendBudget(System.nanoTime(), 0, 1400, 1024 * 1024);

        Assertions.assertTrue(controller.packetsPerSecond() >= 350D);
        Assertions.assertTrue(controller.packetsPerSecond() <= 450D);
    }

    @Test
    public void congestionWindowAndEcnGateSending() {
        final RakNet.Config config = mock(RakNet.Config.class);
        when(config.isAdaptiveTransportEnabled()).thenReturn(true);
        when(config.getMTU()).thenReturn(1400);
        when(config.getAdaptiveMinPps()).thenReturn(50);
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        when(config.getPlpmtudMaxMtu()).thenReturn(1500);
        when(config.getMaxQueuedBytes()).thenReturn(3 * 1024 * 1024);
        when(config.getMetrics()).thenReturn(mock(RakNet.MetricsLogger.class));
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        Assertions.assertTrue(controller.congestionWindowBlocked(controller.congestionWindowBytes(), 1400));
        Assertions.assertFalse(controller.congestionWindowBlocked(0, 1400));
        Assertions.assertEquals(0, controller.sendBudget(System.nanoTime(), controller.congestionWindowBytes(), 1400));
        for (int i = 0; i < 4; i++) controller.onAck(1200, 20_000_000L, 0);
        for (int i = 0; i < 2; i++) controller.onEcnCe();
        Assertions.assertEquals(AdaptiveTransportController.LossType.QUEUE, controller.lossType());
        Assertions.assertEquals(AdaptiveTransportController.CongestionMode.DRAIN, controller.congestionMode());
    }

    @Test
    public void nackDuringRttInflationIsClassifiedAsQueueLoss() {
        final RakNet.Config config = adaptiveConfig();
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);

        controller.onAck(1200, 20_000_000L, 0);
        controller.onAck(1200, 200_000_000L, 0);
        controller.onLoss(600, false);

        Assertions.assertEquals(AdaptiveTransportController.LossType.QUEUE, controller.lossType());
        Assertions.assertEquals(AdaptiveTransportController.CongestionMode.DRAIN, controller.congestionMode());
    }

    @Test
    public void returningAckCannotImmediatelyUndoLossPacingReduction() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        controller.onLoss(600, false);
        final double reducedRate = controller.packetsPerSecond();

        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1_000_000L);
        controller.onAck(1200, 20_000_000L, 0);

        Assertions.assertEquals(reducedRate, controller.packetsPerSecond(), 0.01D);
    }

    @Test
    public void quietPathRecoversLossCeilingWithinSeveralRtts() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1024L * 1024L);
        final long now = System.nanoTime();
        setDouble(controller, "packetsPerSecond", 100D);
        setDouble(controller, "lossPacingCeiling", 100D);
        setLong(controller, "smoothedRtt", 70_000_000L);
        setLong(controller, "lastLossNanos", now - 900_000_000L);
        for (int i = 0; i < 5; i++) {
            final long ackNow = System.nanoTime();
            setLong(controller, "lossRecoveryUpdatedNanos", ackNow - 120_000_000L);
            setLong(controller, "pacingRateUpdatedNanos", ackNow - 120_000_000L);
            controller.onAck(1200, 70_000_000L, 0);
        }

        Assertions.assertTrue(controller.packetsPerSecond() >= 300D,
                "five clean RTTs should recover beyond the old per-second ramp");
        Assertions.assertTrue(controller.packetsPerSecond() < 500D,
                "recovery remains a probe and must not restore the stale maximum immediately");
    }

    @Test
    public void probeRttIsDeferredWhileLossIsActive() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        controller.onAck(1200, 20_000_000L, 0);
        setLong(controller, "minRttStamp", System.nanoTime() - 31_000_000_000L);

        controller.onLoss(600, false);
        controller.sendBudget(System.nanoTime(), 0, 1400);

        Assertions.assertNotEquals(AdaptiveTransportController.CongestionMode.PROBE_RTT,
                controller.congestionMode());
    }

    @Test
    public void healthyStaleMinRttStillStartsProbeRtt() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        controller.onAck(1200, 20_000_000L, 0);
        setLong(controller, "minRttStamp", System.nanoTime() - 31_000_000_000L);

        controller.sendBudget(System.nanoTime(), 0, 1400);

        Assertions.assertEquals(AdaptiveTransportController.CongestionMode.PROBE_RTT,
                controller.congestionMode());
    }

    @Test
    public void sustainedHighLossWithoutRttInflationUsesRateLimitMode() {
        final RakNet.Config config = adaptiveConfig();
        when(config.getSmallWriteCoalesceMicros()).thenReturn(500);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        for (int i = 0; i < 62; i++) controller.onAck(600, 20_000_000L, 0);

        controller.onLoss(600, false);
        controller.onAck(600, 20_000_000L, 0);
        controller.onLoss(600, false);

        Assertions.assertEquals(AdaptiveTransportController.LossType.RATE_LIMIT, controller.lossType());
        Assertions.assertFalse(controller.shouldUseFec());
        Assertions.assertEquals(1_500, controller.smallWriteCoalesceMicros());
    }

    @Test
    public void backgroundFecIsSuppressedDuringBulkDrain() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        for (int i = 0; i < 99; i++) controller.onAck(600, 20_000_000L, 0);
        controller.onLoss(600, false);
        setObject(controller, "lossType", AdaptiveTransportController.LossType.RANDOM);

        Assertions.assertTrue(controller.shouldUseFec());
        setBoolean(controller, "burstDrainActive", true);
        Assertions.assertFalse(controller.shouldUseFec());
    }

    @Test
    public void oneLossBurstCannotRepeatedlyCollapsePacingWithinOneRtt() {
        final RakNet.Config config = adaptiveConfig();
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        for (int i = 0; i < 64; i++) controller.onAck(600, 20_000_000L, 0);

        controller.onLoss(600, false);
        controller.onLoss(600, false);
        final double firstCongestionResponseRate = controller.packetsPerSecond();
        for (int i = 0; i < 20; i++) controller.onLoss(600, false);

        Assertions.assertEquals(AdaptiveTransportController.LossType.RATE_LIMIT, controller.lossType());
        Assertions.assertEquals(firstCongestionResponseRate, controller.packetsPerSecond(), 0.01D);
        Assertions.assertTrue(controller.packetsPerSecond() > config.getAdaptiveMinPps());
    }

    @Test
    public void retransmissionTimeoutUsesTwoRttFloorAndExponentialBackoff() {
        final long millis = 1_000_000L;
        Assertions.assertEquals(200 * millis,
                ReliabilityHandler.retransmissionTimeoutNanos(75 * millis, 5 * millis, 50 * millis, 0));
        Assertions.assertEquals(400 * millis,
                ReliabilityHandler.retransmissionTimeoutNanos(75 * millis, 5 * millis, 50 * millis, 1));
        Assertions.assertEquals(205 * millis,
                ReliabilityHandler.retransmissionTimeoutNanos(75 * millis, 20 * millis, 50 * millis, 0));
    }

    @Test
    public void ackCompressionCannotInstantlyJumpToMaximumPacing() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1_000_000L);
        setLong(controller, "pacingRateUpdatedNanos", System.nanoTime() - 100_000_000L);

        controller.onAck(1200, 20_000_000L, 0);

        Assertions.assertTrue(controller.packetsPerSecond() < 600D,
                "100ms of growth must not jump from 500pps to the 2000pps maximum");
    }

    @Test
    public void ackCompressionCannotInflateRetainedPathCapacityBeyondSendRate() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        setLong(controller, "deliverySampleStarted", System.nanoTime() - 100_000_000L);
        setLong(controller, "deliverySampleBytes", 10L * 1024L * 1024L);

        controller.onAck(1200, 20_000_000L, 0, false, Long.MIN_VALUE);

        Assertions.assertTrue(controller.validatedPathRateBytesPerSecond() <= 600L * 1024L,
                "compressed ACKs must be capped by the sender's actual pacing rate");
    }

    @Test
    public void ecnFeedbackIsIgnoredWhenAdaptiveTransportIsDisabled() {
        final RakNet.Config config = mock(RakNet.Config.class);
        when(config.isAdaptiveTransportEnabled()).thenReturn(false);
        when(config.getMTU()).thenReturn(1400);
        when(config.getAdaptiveMinPps()).thenReturn(50);
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        when(config.getPlpmtudMaxMtu()).thenReturn(1500);
        when(config.getMaxQueuedBytes()).thenReturn(3 * 1024 * 1024);
        when(config.getMetrics()).thenReturn(mock(RakNet.MetricsLogger.class));
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        final long initialWindow = controller.congestionWindowBytes();

        for (int i = 0; i < 10; i++) controller.onEcnCe();

        Assertions.assertEquals(AdaptiveTransportController.LossType.NONE, controller.lossType());
        Assertions.assertEquals(AdaptiveTransportController.CongestionMode.STARTUP, controller.congestionMode());
        Assertions.assertEquals(initialWindow, controller.congestionWindowBytes());
    }

    @Test
    public void adaptiveTuningOptionsRoundTripAndValidate() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final DefaultConfig config = new DefaultConfig(channel);
        Assertions.assertTrue(config.setOption(RakNet.ADAPTIVE_MIN_PPS, 75));
        Assertions.assertTrue(config.setOption(RakNet.ADAPTIVE_MAX_PPS, 1500));
        Assertions.assertTrue(config.setOption(RakNet.SMALL_WRITE_COALESCE_MICROS, 400));
        Assertions.assertTrue(config.setOption(RakNet.PLPMTUD_MAX_MTU, 9000));
        Assertions.assertEquals(75, config.getOption(RakNet.ADAPTIVE_MIN_PPS));
        Assertions.assertEquals(1500, config.getOption(RakNet.ADAPTIVE_MAX_PPS));
        Assertions.assertEquals(400, config.getOption(RakNet.SMALL_WRITE_COALESCE_MICROS));
        Assertions.assertEquals(9000, config.getOption(RakNet.PLPMTUD_MAX_MTU));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> config.setOption(RakNet.SMALL_WRITE_COALESCE_MICROS, -1));
        channel.finishAndReleaseAll();
    }

    private static RakNet.Config adaptiveConfig() {
        final RakNet.Config config = mock(RakNet.Config.class);
        when(config.isAdaptiveTransportEnabled()).thenReturn(true);
        when(config.getMTU()).thenReturn(1400);
        when(config.getAdaptiveMinPps()).thenReturn(50);
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        when(config.getPlpmtudMaxMtu()).thenReturn(1500);
        when(config.getMaxQueuedBytes()).thenReturn(3 * 1024 * 1024);
        when(config.getMetrics()).thenReturn(mock(RakNet.MetricsLogger.class));
        return config;
    }

    private static void setLong(Object target, String name, long value) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.setLong(target, value);
    }

    private static void setDouble(Object target, String name, double value) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.setDouble(target, value);
    }

    private static void setBoolean(Object target, String name, boolean value) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.setBoolean(target, value);
    }

    private static void setObject(Object target, String name, Object value) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
