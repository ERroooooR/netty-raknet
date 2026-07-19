package network.ycc.raknet.utils;

/** Overflow-safe arithmetic for non-negative transport times and byte counts. */
public final class SaturatedMath {
    private SaturatedMath() {
    }

    public static long add(long a, long b) {
        if (a <= 0L) return Math.max(0L, b);
        if (b <= 0L) return a;
        return a > Long.MAX_VALUE - b ? Long.MAX_VALUE : a + b;
    }

    public static long multiply(long a, long b) {
        if (a <= 0L || b <= 0L) return 0L;
        return a > Long.MAX_VALUE / b ? Long.MAX_VALUE : a * b;
    }
}
