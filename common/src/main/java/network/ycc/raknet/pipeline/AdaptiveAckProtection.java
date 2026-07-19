package network.ycc.raknet.pipeline;

import network.ycc.raknet.utils.SaturatedMath;

import java.util.concurrent.TimeUnit;

/** Activates bounded ACK repetition only after evidence that ACKs are being lost. */
final class AdaptiveAckProtection {
    private static final long MIN_REPEAT_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
    private static final long MAX_REPEAT_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(20);
    private static final long MIN_PROTECTION_NANOS = TimeUnit.SECONDS.toNanos(2);
    private static final int DUPLICATE_THRESHOLD = 3;
    private static final long TRIGGER_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(1);

    private int duplicateScore;
    private long triggerWindowStartedNanos;
    private long protectedUntilNanos;

    void onDuplicateFrameSet(long nowNanos, long rttNanos) {
        if (isActive(nowNanos)) {
            extendProtection(nowNanos, rttNanos);
            return;
        }
        if (triggerWindowStartedNanos == 0L
                || nowNanos - triggerWindowStartedNanos > TRIGGER_WINDOW_NANOS) {
            triggerWindowStartedNanos = nowNanos;
            duplicateScore = 0;
        }
        duplicateScore++;
        if (duplicateScore >= DUPLICATE_THRESHOLD) extendProtection(nowNanos, rttNanos);
    }

    boolean isActive(long nowNanos) {
        return nowNanos < protectedUntilNanos;
    }

    long repeatDelayNanos(long rttNanos) {
        final long candidate = Math.max(0L, rttNanos) / 8L;
        return Math.max(MIN_REPEAT_DELAY_NANOS,
                Math.min(MAX_REPEAT_DELAY_NANOS, candidate));
    }

    private void extendProtection(long nowNanos, long rttNanos) {
        final long pathProtection = SaturatedMath.multiply(Math.max(0L, rttNanos), 8L);
        protectedUntilNanos = SaturatedMath.add(nowNanos,
                Math.max(MIN_PROTECTION_NANOS, pathProtection));
    }
}
