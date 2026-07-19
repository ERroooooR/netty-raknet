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
        final DeferredNackTracker tracker = new DeferredNackTracker();
        final List<Integer> due = new ArrayList<>();
        Assertions.assertTrue(tracker.defer(10, 1_000));
        Assertions.assertTrue(tracker.cancel(10));
        Assertions.assertEquals(-1L, tracker.drainDue(2_000, due::add));
        Assertions.assertTrue(due.isEmpty());
    }

    @Test
    public void onlyExpiredDeferredNacksAreDrained() {
        final DeferredNackTracker tracker = new DeferredNackTracker();
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
        final DeferredNackTracker tracker = new DeferredNackTracker();
        final List<Integer> confirmed = new ArrayList<>();
        tracker.defer(10, 10_000);
        tracker.defer(11, 10_000);

        tracker.drainAll(confirmed::add);

        Assertions.assertEquals(2, confirmed.size());
        Assertions.assertEquals(-1L, tracker.drainDue(Long.MAX_VALUE, confirmed::add));
    }

    @Test
    public void repeatedTrueLossTemporarilyBypassesReorderGrace() {
        final AdaptiveNackGrace policy = new AdaptiveNackGrace();
        final long rtt = TimeUnit.MILLISECONDS.toNanos(80);
        for (int i = 0; i < 8; i++) {
            policy.onLost(i + 1L, rtt);
        }

        Assertions.assertTrue(policy.isBypassing(9L));
        Assertions.assertFalse(policy.shouldDefer(9L));
    }

    @Test
    public void oneReorderInInitialWindowPreventsPrematureBypass() {
        final AdaptiveNackGrace policy = new AdaptiveNackGrace();
        final long rtt = TimeUnit.MILLISECONDS.toNanos(80);
        for (int i = 0; i < 7; i++) policy.onLost(i + 1L, rtt);
        policy.onReordered(8L);

        Assertions.assertFalse(policy.isBypassing(9L));
        Assertions.assertTrue(policy.shouldDefer(9L));
    }

    @Test
    public void bypassUsesOneProbeAndImmediatelyReentersOnTrueLoss() {
        final AdaptiveNackGrace policy = new AdaptiveNackGrace();
        final long rtt = TimeUnit.MILLISECONDS.toNanos(80);
        for (int i = 0; i < 8; i++) policy.onLost(i + 1L, rtt);

        final long afterCooldown = TimeUnit.SECONDS.toNanos(3);
        Assertions.assertTrue(policy.shouldDefer(afterCooldown));
        Assertions.assertFalse(policy.shouldDefer(afterCooldown + 1L));
        policy.onLost(afterCooldown + 2L, rtt);

        Assertions.assertTrue(policy.isBypassing(afterCooldown + 3L));
        Assertions.assertFalse(policy.shouldDefer(afterCooldown + 3L));
    }

    @Test
    public void successfulProbeRestoresReorderGrace() {
        final AdaptiveNackGrace policy = new AdaptiveNackGrace();
        final long rtt = TimeUnit.MILLISECONDS.toNanos(80);
        for (int i = 0; i < 8; i++) policy.onLost(i + 1L, rtt);

        final long afterCooldown = TimeUnit.SECONDS.toNanos(3);
        Assertions.assertTrue(policy.shouldDefer(afterCooldown));
        policy.onReordered(afterCooldown + 1L);

        Assertions.assertFalse(policy.isBypassing(afterCooldown + 2L));
        Assertions.assertTrue(policy.shouldDefer(afterCooldown + 2L));
    }

    @Test
    public void ackProtectionRequiresDuplicateBurstAndExpires() {
        final AdaptiveAckProtection policy = new AdaptiveAckProtection();
        final long rtt = TimeUnit.MILLISECONDS.toNanos(80);
        final long now = TimeUnit.SECONDS.toNanos(1);

        policy.onDuplicateFrameSet(now, rtt);
        policy.onDuplicateFrameSet(now + 1L, rtt);
        Assertions.assertFalse(policy.isActive(now + 2L));
        policy.onDuplicateFrameSet(now + 2L, rtt);

        Assertions.assertTrue(policy.isActive(now + 3L));
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(10),
                policy.repeatDelayNanos(rtt));
        Assertions.assertFalse(policy.isActive(now + TimeUnit.SECONDS.toNanos(3)));
    }

    @Test
    public void sparseDuplicateFrameSetsDoNotActivateAckProtection() {
        final AdaptiveAckProtection policy = new AdaptiveAckProtection();
        final long rtt = TimeUnit.MILLISECONDS.toNanos(80);
        policy.onDuplicateFrameSet(1L, rtt);
        policy.onDuplicateFrameSet(TimeUnit.SECONDS.toNanos(2), rtt);
        policy.onDuplicateFrameSet(TimeUnit.SECONDS.toNanos(4), rtt);

        Assertions.assertFalse(policy.isActive(TimeUnit.SECONDS.toNanos(4) + 1L));
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(5),
                policy.repeatDelayNanos(TimeUnit.MILLISECONDS.toNanos(10)));
        Assertions.assertEquals(TimeUnit.MILLISECONDS.toNanos(20),
                policy.repeatDelayNanos(TimeUnit.SECONDS.toNanos(1)));
    }

    @Test
    public void duplicateDuringProtectionExtendsQuietDeadline() {
        final AdaptiveAckProtection policy = new AdaptiveAckProtection();
        final long rtt = TimeUnit.MILLISECONDS.toNanos(80);
        final long now = TimeUnit.SECONDS.toNanos(1);
        policy.onDuplicateFrameSet(now, rtt);
        policy.onDuplicateFrameSet(now + 1L, rtt);
        policy.onDuplicateFrameSet(now + 2L, rtt);
        final long nearOriginalExpiry = now + TimeUnit.MILLISECONDS.toNanos(1900);
        policy.onDuplicateFrameSet(nearOriginalExpiry, rtt);

        Assertions.assertTrue(policy.isActive(now + TimeUnit.SECONDS.toNanos(3)));
        Assertions.assertFalse(policy.isActive(nearOriginalExpiry
                + TimeUnit.SECONDS.toNanos(3)));
    }
}
