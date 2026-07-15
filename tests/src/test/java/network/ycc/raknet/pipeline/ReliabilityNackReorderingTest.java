package network.ycc.raknet.pipeline;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReliabilityNackReorderingTest {
    @Test
    public void onlySmallIsolatedGapsAreDeferred() {
        Assertions.assertFalse(ReliabilityHandler.shouldDeferNackGap(0));
        Assertions.assertTrue(ReliabilityHandler.shouldDeferNackGap(1));
        Assertions.assertTrue(ReliabilityHandler.shouldDeferNackGap(2));
        Assertions.assertFalse(ReliabilityHandler.shouldDeferNackGap(3));
    }

    @Test
    public void reorderDelayUsesRttAndJitterWithinStrictBounds() {
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(4),
                ReliabilityHandler.nackReorderDelayNanos(0, 0));
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(10),
                ReliabilityHandler.nackReorderDelayNanos(
                        TimeUnit.MILLISECONDS.toNanos(80), TimeUnit.MILLISECONDS.toNanos(4)));
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(25),
                ReliabilityHandler.nackReorderDelayNanos(
                        TimeUnit.MILLISECONDS.toNanos(80), TimeUnit.MILLISECONDS.toNanos(80)));
    }

    @Test
    public void reorderedPacketCancelsBeforeNackBecomesVisible() {
        final ReliabilityHandler.DeferredNackTracker tracker =
                new ReliabilityHandler.DeferredNackTracker();
        final List<Integer> due = new ArrayList<>();
        Assertions.assertTrue(tracker.defer(10, 1_000));
        Assertions.assertTrue(tracker.cancel(10));
        Assertions.assertEquals(-1L, tracker.drainDue(2_000, due::add));
        Assertions.assertTrue(due.isEmpty());
    }

    @Test
    public void onlyExpiredDeferredNacksAreDrained() {
        final ReliabilityHandler.DeferredNackTracker tracker =
                new ReliabilityHandler.DeferredNackTracker();
        final List<Integer> due = new ArrayList<>();
        tracker.defer(10, 1_000);
        tracker.defer(11, 2_000);

        Assertions.assertEquals(1_000L, tracker.drainDue(1_000, due::add));
        Assertions.assertEquals(List.of(10), due);
        tracker.drainDue(2_000, due::add);
        Assertions.assertEquals(List.of(10, 11), due);
    }

    @Test
    public void confirmedLargeGapCanDrainAllPendingNacksImmediately() {
        final ReliabilityHandler.DeferredNackTracker tracker =
                new ReliabilityHandler.DeferredNackTracker();
        final List<Integer> confirmed = new ArrayList<>();
        tracker.defer(10, 10_000);
        tracker.defer(11, 10_000);

        tracker.drainAll(confirmed::add);

        Assertions.assertEquals(2, confirmed.size());
        Assertions.assertEquals(-1L, tracker.drainDue(Long.MAX_VALUE, confirmed::add));
    }
}
