package network.ycc.raknet.pipeline;

import java.util.List;

/** Small systematic GF(256) codec optimized for one or two RakNet parity shards. */
final class ReedSolomonCodec {
    private static final int[] EXP = new int[512];
    private static final int[] LOG = new int[256];

    static {
        int value = 1;
        for (int i = 0; i < 255; i++) {
            EXP[i] = value;
            LOG[value] = i;
            value <<= 1;
            if ((value & 0x100) != 0) value ^= 0x11d;
        }
        for (int i = 255; i < EXP.length; i++) EXP[i] = EXP[i - 255];
    }

    private ReedSolomonCodec() {}

    static byte[][] encode(List<byte[]> data, int parityShards) {
        int size = 0;
        for (byte[] shard : data) size = Math.max(size, shard.length);
        final byte[][] parity = new byte[parityShards][size];
        for (int p = 0; p < parityShards; p++) {
            for (int d = 0; d < data.size(); d++) {
                final int coefficient = coefficient(p, d);
                final byte[] shard = data.get(d);
                for (int i = 0; i < shard.length; i++) {
                    parity[p][i] ^= (byte) multiply(shard[i] & 0xff, coefficient);
                }
            }
        }
        return parity;
    }

    static byte[][] recover(byte[][] data, int[] lengths, byte[][] parity) {
        int missingCount = 0;
        for (byte[] shard : data) if (shard == null) missingCount++;
        if (missingCount == 0) return new byte[0][];
        int availableParity = 0;
        for (byte[] shard : parity) if (shard != null) availableParity++;
        if (availableParity < missingCount) return null;

        final int[] missing = new int[missingCount];
        final int[] rows = new int[missingCount];
        for (int i = 0, m = 0; i < data.length; i++) if (data[i] == null) missing[m++] = i;
        for (int i = 0, r = 0; i < parity.length && r < missingCount; i++) if (parity[i] != null) rows[r++] = i;

        final int[][] matrix = new int[missingCount][missingCount];
        for (int r = 0; r < missingCount; r++) {
            for (int c = 0; c < missingCount; c++) matrix[r][c] = coefficient(rows[r], missing[c]);
        }
        final int[][] inverse = invert(matrix);
        if (inverse == null) return null;
        final byte[][] recovered = new byte[missingCount][];
        for (int m = 0; m < missingCount; m++) recovered[m] = new byte[lengths[missing[m]]];

        int max = 0;
        for (int length : lengths) max = Math.max(max, length);
        final int[] rhs = new int[missingCount];
        for (int pos = 0; pos < max; pos++) {
            for (int r = 0; r < missingCount; r++) {
                int value = pos < parity[rows[r]].length ? parity[rows[r]][pos] & 0xff : 0;
                for (int d = 0; d < data.length; d++) {
                    if (data[d] != null && pos < data[d].length) {
                        value ^= multiply(data[d][pos] & 0xff, coefficient(rows[r], d));
                    }
                }
                rhs[r] = value;
            }
            for (int m = 0; m < missingCount; m++) {
                int value = 0;
                for (int r = 0; r < missingCount; r++) value ^= multiply(inverse[m][r], rhs[r]);
                if (pos < recovered[m].length) recovered[m][pos] = (byte) value;
            }
        }
        return recovered;
    }

    private static int coefficient(int parityRow, int dataColumn) {
        if (parityRow == 0) return 1;
        return power(dataColumn + 1, parityRow);
    }

    private static int[][] invert(int[][] input) {
        final int n = input.length;
        final int[][] augmented = new int[n][n * 2];
        for (int r = 0; r < n; r++) {
            System.arraycopy(input[r], 0, augmented[r], 0, n);
            augmented[r][n + r] = 1;
        }
        for (int column = 0; column < n; column++) {
            int pivot = column;
            while (pivot < n && augmented[pivot][column] == 0) pivot++;
            if (pivot == n) return null;
            final int[] swap = augmented[column]; augmented[column] = augmented[pivot]; augmented[pivot] = swap;
            final int inverse = divide(1, augmented[column][column]);
            for (int c = 0; c < n * 2; c++) augmented[column][c] = multiply(augmented[column][c], inverse);
            for (int r = 0; r < n; r++) {
                if (r == column) continue;
                final int factor = augmented[r][column];
                if (factor == 0) continue;
                for (int c = 0; c < n * 2; c++) augmented[r][c] ^= multiply(factor, augmented[column][c]);
            }
        }
        final int[][] inverse = new int[n][n];
        for (int r = 0; r < n; r++) System.arraycopy(augmented[r], n, inverse[r], 0, n);
        return inverse;
    }

    private static int multiply(int a, int b) {
        if (a == 0 || b == 0) return 0;
        return EXP[LOG[a] + LOG[b]];
    }

    private static int divide(int a, int b) {
        if (b == 0) throw new ArithmeticException("division by zero in GF(256)");
        if (a == 0) return 0;
        return EXP[(LOG[a] - LOG[b] + 255) % 255];
    }

    private static int power(int value, int exponent) {
        if (exponent == 0) return 1;
        if (value == 0) return 0;
        return EXP[(LOG[value] * exponent) % 255];
    }
}
