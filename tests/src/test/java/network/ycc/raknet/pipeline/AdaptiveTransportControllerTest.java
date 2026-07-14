package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultConfig;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AdaptiveTransportControllerTest {
    @Test
    public void defaultConfigUsesPublicInternetPpsCeiling() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final DefaultConfig config = new DefaultConfig(channel);
        Assertions.assertEquals(600, config.getAdaptiveMaxPps());
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
        Assertions.assertEquals(0, controller.sendBudget(now));
        Assertions.assertEquals(0, controller.sendBudget(now + 2_000_000L));
        Assertions.assertEquals(1, controller.sendBudget(now + 3_000_000L));
        Assertions.assertTrue(controller.nanosUntilSend(now) >= 0);
    }

    @Test
    public void actualDatagramSizeCreatesByteDebtAfterSmallEstimate() {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long now = System.nanoTime();
        Assertions.assertEquals(1, controller.sendBudget(now, 0, 100));
        controller.onDatagramSent(1_400);

        Assertions.assertEquals(0, controller.sendBudget(now + 1_000_000L, 0, 100));
        Assertions.assertEquals(1, controller.sendBudget(now + 3_000_000L, 0, 100));
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
    public void severeLossCeilingDoesNotRecoverDuringTwentySecondQuietPeriod() throws Exception {
        final RakNet.Config config = adaptiveConfig();
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);
        for (int i = 0; i < 64; i++) controller.onAck(600, 20_000_000L, 0);
        controller.onLoss(600, false);
        controller.onLoss(600, false);
        final double reducedRate = controller.packetsPerSecond();

        final Field bandwidth = AdaptiveTransportController.class.getDeclaredField("bandwidthFilter");
        bandwidth.setAccessible(true);
        Arrays.fill((long[]) bandwidth.get(controller), 1_000_000L);
        setLong(controller, "lastLossNanos", System.nanoTime() - 10_000_000_000L);
        controller.onAck(1200, 20_000_000L, 0);

        Assertions.assertEquals(reducedRate, controller.packetsPerSecond(), 0.01D);
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
    public void nackReorderDelayScalesWithRttAndIsBounded() {
        final long millis = 1_000_000L;
        Assertions.assertEquals(3 * millis, ReliabilityHandler.nackReorderDelayNanos(0));
        Assertions.assertEquals(5 * millis, ReliabilityHandler.nackReorderDelayNanos(40 * millis));
        Assertions.assertEquals(12 * millis, ReliabilityHandler.nackReorderDelayNanos(200 * millis));
    }

    @Test
    public void deferredNackIsCancelledByReorderedArrivalBeforeDeadline() {
        final ReliabilityHandler.DeferredNackTracker tracker =
                new ReliabilityHandler.DeferredNackTracker();
        final List<Integer> due = new ArrayList<>();
        Assertions.assertTrue(tracker.defer(10, 5_000L));
        Assertions.assertTrue(tracker.cancel(10));
        Assertions.assertEquals(-1L, tracker.drainDue(10_000L, due::add));
        Assertions.assertTrue(due.isEmpty());
    }

    @Test
    public void deferredNackPromotesOnlyExpiredSequenceGaps() {
        final ReliabilityHandler.DeferredNackTracker tracker =
                new ReliabilityHandler.DeferredNackTracker();
        final List<Integer> due = new ArrayList<>();
        tracker.defer(10, 5_000L);
        tracker.defer(11, 8_000L);

        Assertions.assertEquals(3_000L, tracker.drainDue(5_000L, due::add));
        Assertions.assertEquals(Arrays.asList(10), due);
        due.clear();
        Assertions.assertEquals(-1L, tracker.drainDue(8_000L, due::add));
        Assertions.assertEquals(Arrays.asList(11), due);
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
    public void deliveryEstimatorAggregatesForOneHundredMillisecondsAndRejectsAckSpike() {
        final AdaptiveTransportController controller = new AdaptiveTransportController(adaptiveConfig());
        final long second = 1_000_000_000L;
        controller.updateDeliveryRate(second, 1_200);
        controller.updateDeliveryRate(second + 50_000_000L, 1_200);
        Assertions.assertEquals(0L, controller.bandwidthEstimateBytesPerSecond());

        controller.updateDeliveryRate(second + 100_000_000L, 1_200);
        final long baseline = controller.bandwidthEstimateBytesPerSecond();
        Assertions.assertEquals(36_000L, baseline);

        controller.updateDeliveryRate(second + 200_000_000L, 1_000_000);
        Assertions.assertTrue(controller.bandwidthEstimateBytesPerSecond() < baseline * 1.05D,
                "a compressed ACK burst must not become the sustained bandwidth estimate");
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
}
