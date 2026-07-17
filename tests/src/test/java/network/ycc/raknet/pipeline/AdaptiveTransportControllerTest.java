package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultConfig;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
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
    public void failedProbeNarrowsSearchWithoutReducingConfirmedMtu() {
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
        verify(config).setMTU(1200);
        Assertions.assertEquals(1200, controller.probeCandidate());
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
    public void learnedHealthyBacklogCanExceedLegacyCeiling() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        for (int i = 0; i < 64; i++) controller.onAck(1200, 40_000_000L, 0);
        setDouble(controller, "packetsPerSecond", 400D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 400 * 1024);

        Assertions.assertTrue(controller.packetsPerSecond() >= 700D);
        Assertions.assertTrue(controller.packetsPerSecond() <= 900D);
    }

    @Test
    public void newConnectionCannotJumpStraightToTwoThousandPps() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        setDouble(controller, "packetsPerSecond", 50D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 4 * 1024 * 1024);

        Assertions.assertEquals(600D, controller.packetsPerSecond(), 0.01D);
    }

    @Test
    public void newBacklogUsesBoundedByteBurstInsteadOfPpsOnly() {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();

        Assertions.assertEquals(1, controller.sendBudget(now, 0, 1400, 1024 * 1024));
        Assertions.assertEquals(0, controller.sendBudget(now, 0, 1400, 1024 * 1024));
        Assertions.assertEquals(384L * 1024L, controller.burstAdmissionRateBytesPerSecond());
        Assertions.assertTrue(controller.sendBudget(now + 10_000_000L, 0, 1400, 1024 * 1024) <= 2,
                "the first 10ms must not release the full four-datagram PPS burst");
    }

    @Test
    public void cleanAcksRampBurstAdmissionAtRttCadence() throws Exception {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        controller.sendBudget(now, 0, 1400, 1024 * 1024);
        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 2L * 1024L * 1024L);
        setLong(controller, "smoothedRtt", 40_000_000L);
        setLong(controller, "lastAckNanos", now);
        setLong(controller, "burstRampUpdatedNanos", now - 160_000_000L);

        controller.sendBudget(now, 0, 1400, 1024 * 1024);

        Assertions.assertTrue(controller.burstAdmissionRateBytesPerSecond() >= 900L * 1024L);
        Assertions.assertTrue(controller.burstRecoveryProbes() >= 4L);
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
        Assertions.assertTrue(controller.packetsPerSecond() >= 120D);

        setDouble(controller, "packetsPerSecond", 50D);
        controller.sendBudget(System.nanoTime(), 64 * 1024, 1400, 0);
        Assertions.assertTrue(controller.packetsPerSecond() >= 120D);
    }

    @Test
    public void observedLargeZstdBatchGetsSubSecondDrainRate() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        when(config.getAdaptiveMaxPps()).thenReturn(600);
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        setDouble(controller, "packetsPerSecond", 50D);

        controller.sendBudget(System.nanoTime(), 0, 1400, 211 * 1024);

        Assertions.assertTrue(controller.packetsPerSecond() >= 400D);
        Assertions.assertTrue(controller.packetsPerSecond() <= 600D);
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

    private static void setObject(Object target, String name, Object value) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
