package network.ycc.raknet.pipeline;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Event-loop-confined rolling packet-loss and ECN observations. */
final class TransportLossWindow {
    private static final int BUCKETS = 10;
    private static final long BUCKET_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final long[] acknowledged = new long[BUCKETS];
    private final long[] lost = new long[BUCKETS];
    private final long[] ecnMarks = new long[BUCKETS];
    private int bucket;
    private long bucketStarted = System.nanoTime();

    void recordAcknowledgement(long nowNanos) {
        rotate(nowNanos);
        acknowledged[bucket]++;
    }

    void recordLoss(long nowNanos) {
        rotate(nowNanos);
        lost[bucket]++;
    }

    void recordEcn(long nowNanos) {
        rotate(nowNanos);
        ecnMarks[bucket]++;
    }

    double lossRatio(long nowNanos) {
        rotate(nowNanos);
        return currentLossRatio();
    }

    double currentLossRatio() {
        final long acknowledgedCount = acknowledgedCount();
        final long lostCount = lostCount();
        return acknowledgedCount + lostCount == 0L ? 0D
                : lostCount / (double) (acknowledgedCount + lostCount);
    }

    double currentEcnRatio() {
        long acknowledgements = 0L;
        long marks = 0L;
        for (int i = 0; i < BUCKETS; i++) {
            acknowledgements += acknowledged[i];
            marks += ecnMarks[i];
        }
        return acknowledgements + marks == 0L ? 0D
                : marks / (double) (acknowledgements + marks);
    }

    long sampleCount() {
        return acknowledgedCount() + lostCount();
    }

    long acknowledgedCount() {
        long count = 0L;
        for (long value : acknowledged) count += value;
        return count;
    }

    long lostCount() {
        long count = 0L;
        for (long value : lost) count += value;
        return count;
    }

    private void rotate(long nowNanos) {
        long elapsed = (nowNanos - bucketStarted) / BUCKET_NANOS;
        if (elapsed >= BUCKETS) {
            Arrays.fill(acknowledged, 0L);
            Arrays.fill(lost, 0L);
            Arrays.fill(ecnMarks, 0L);
            bucket = 0;
            bucketStarted = nowNanos;
            return;
        }
        while (elapsed-- > 0L) {
            bucket = (bucket + 1) % BUCKETS;
            acknowledged[bucket] = 0L;
            lost[bucket] = 0L;
            ecnMarks[bucket] = 0L;
            bucketStarted += BUCKET_NANOS;
        }
    }
}
