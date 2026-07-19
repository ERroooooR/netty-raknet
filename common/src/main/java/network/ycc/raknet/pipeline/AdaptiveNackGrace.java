package network.ycc.raknet.pipeline;

import network.ycc.raknet.utils.SaturatedMath;

import java.util.concurrent.TimeUnit;

/** Learns whether short sequence gaps are reordering or true loss. */
final class AdaptiveNackGrace {
    private static final int OUTCOME_WINDOW = 32;
    private static final int MIN_OUTCOMES = 8;
    private static final int BYPASS_PERCENT = 88;
    private static final long MIN_BYPASS_NANOS = TimeUnit.SECONDS.toNanos(2);

    private long lossOutcomes;
    private int outcomeCount;
    private int outcomeIndex;
    private int lossCount;
    private boolean bypass;
    private boolean probePending;
    private long bypassUntilNanos;

    boolean shouldDefer(long nowNanos) {
        if (!bypass) return true;
        if (probePending || nowNanos < bypassUntilNanos) return false;
        probePending = true;
        return true;
    }

    void onLost(long nowNanos, long rttNanos) {
        if (probePending) {
            probePending = false;
            enterBypass(nowNanos, rttNanos);
            return;
        }
        recordOutcome(true);
        if (outcomeCount >= MIN_OUTCOMES
                && lossCount * 100 >= outcomeCount * BYPASS_PERCENT) {
            enterBypass(nowNanos, rttNanos);
        }
    }

    void onReordered(long nowNanos) {
        if (probePending) {
            probePending = false;
            bypass = false;
            bypassUntilNanos = 0L;
            clearOutcomes();
        }
        recordOutcome(false);
    }

    boolean isBypassing(long nowNanos) {
        return bypass && (probePending || nowNanos < bypassUntilNanos);
    }

    private void enterBypass(long nowNanos, long rttNanos) {
        bypass = true;
        final long pathCooldown = SaturatedMath.multiply(Math.max(0L, rttNanos), 16L);
        bypassUntilNanos = SaturatedMath.add(nowNanos,
                Math.max(MIN_BYPASS_NANOS, pathCooldown));
    }

    private void recordOutcome(boolean lost) {
        final long bit = 1L << outcomeIndex;
        if (outcomeCount == OUTCOME_WINDOW) {
            if ((lossOutcomes & bit) != 0L) lossCount--;
        } else {
            outcomeCount++;
        }
        if (lost) {
            lossOutcomes |= bit;
            lossCount++;
        } else {
            lossOutcomes &= ~bit;
        }
        outcomeIndex = (outcomeIndex + 1) % OUTCOME_WINDOW;
    }

    private void clearOutcomes() {
        lossOutcomes = 0L;
        outcomeCount = 0;
        outcomeIndex = 0;
        lossCount = 0;
    }
}
