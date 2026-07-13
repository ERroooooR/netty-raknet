package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultConfig;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AdaptiveTransportControllerTest {
    @Test
    public void failedProbeNarrowsSearchWithoutReducingConfirmedMtu() {
        final RakNet.Config config = mock(RakNet.Config.class);
        when(config.isAdaptiveTransportEnabled()).thenReturn(true);
        when(config.getMTU()).thenReturn(1400);
        when(config.getAdaptiveMinPps()).thenReturn(50);
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
        when(config.getMetrics()).thenReturn(mock(RakNet.MetricsLogger.class));
        final AdaptiveTransportController controller = new AdaptiveTransportController(config);

        controller.onLoss(1400, true);
        controller.onAck(128, 20_000_000L);
        controller.onLoss(1400, true);
        controller.onAck(128, 20_000_000L);
        controller.onLoss(1400, true);
        controller.applyPendingMtu();
        verify(config).setMTU(1368);

        Assertions.assertEquals(1400, controller.probeCandidate());
        controller.onProbeTimeout(1400);
        Assertions.assertEquals(1399, controller.probeCandidate());
    }

    @Test
    public void pacingNeverReturnsNegativeDelay() {
        final RakNet.Config config = mock(RakNet.Config.class);
        when(config.isAdaptiveTransportEnabled()).thenReturn(true);
        when(config.getMTU()).thenReturn(1400);
        when(config.getAdaptiveMinPps()).thenReturn(50);
        when(config.getAdaptiveMaxPps()).thenReturn(2000);
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
    public void adaptiveTuningOptionsRoundTripAndValidate() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final DefaultConfig config = new DefaultConfig(channel);
        Assertions.assertTrue(config.setOption(RakNet.ADAPTIVE_MIN_PPS, 75));
        Assertions.assertTrue(config.setOption(RakNet.ADAPTIVE_MAX_PPS, 1500));
        Assertions.assertTrue(config.setOption(RakNet.SMALL_WRITE_COALESCE_MICROS, 400));
        Assertions.assertEquals(75, config.getOption(RakNet.ADAPTIVE_MIN_PPS));
        Assertions.assertEquals(1500, config.getOption(RakNet.ADAPTIVE_MAX_PPS));
        Assertions.assertEquals(400, config.getOption(RakNet.SMALL_WRITE_COALESCE_MICROS));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> config.setOption(RakNet.SMALL_WRITE_COALESCE_MICROS, -1));
        channel.finishAndReleaseAll();
    }
}
