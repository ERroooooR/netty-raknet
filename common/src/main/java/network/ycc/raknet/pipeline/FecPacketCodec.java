package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/** Bounded wire parser and XOR primitives for the negotiated FEC extension. */
final class FecPacketCodec {
    private static final int MIN_GROUP_SIZE = 4;
    private static final int MAX_GROUP_SIZE = 12;
    private static final int LEGACY_MAX_PROTECTED_BYTES = 512;
    private static final int MAX_DATAGRAM_BYTES = 65_507;

    private FecPacketCodec() {
    }

    static LegacyParity readLegacyParity(ByteBuf in) {
        in.skipBytes(1);
        final int id = in.readInt();
        final Entry[] entries = readEntries(in, in.readUnsignedByte(), LEGACY_MAX_PROTECTED_BYTES);
        final int size = in.readUnsignedShort();
        if (size > LEGACY_MAX_PROTECTED_BYTES || in.readableBytes() != size) {
            throw new IllegalArgumentException("invalid XOR parity size");
        }
        for (Entry entry : entries) {
            if (entry.length > size) throw new IllegalArgumentException("FEC entry exceeds parity");
        }
        final byte[] parity = new byte[size];
        in.readBytes(parity);
        return new LegacyParity(id, entries, parity, System.nanoTime());
    }

    static RsParity readReedSolomonParity(ByteBuf in) {
        in.skipBytes(1);
        final int id = in.readInt();
        final int dataCount = in.readUnsignedByte();
        final int parityCount = in.readUnsignedByte();
        final int parityIndex = in.readUnsignedByte();
        if (parityCount < 1 || parityCount > 2 || parityIndex >= parityCount) {
            throw new IllegalArgumentException("invalid parity shard");
        }
        final Entry[] entries = readEntries(in, dataCount, MAX_DATAGRAM_BYTES);
        final int size = in.readUnsignedShort();
        if (size > MAX_DATAGRAM_BYTES || in.readableBytes() != size) {
            throw new IllegalArgumentException("invalid Reed-Solomon parity size");
        }
        for (Entry entry : entries) {
            if (entry.length > size) throw new IllegalArgumentException("FEC entry exceeds parity");
        }
        final byte[] parity = new byte[size];
        in.readBytes(parity);
        return new RsParity(id, entries, parityCount, parityIndex, parity, System.nanoTime());
    }

    private static Entry[] readEntries(ByteBuf in, int count, int maximumLength) {
        if (count < MIN_GROUP_SIZE || count > MAX_GROUP_SIZE) {
            throw new IllegalArgumentException("invalid FEC group size");
        }
        final Entry[] entries = new Entry[count];
        final HashSet<Integer> sequences = new HashSet<>();
        for (int i = 0; i < count; i++) {
            final int seq = in.readUnsignedMediumLE();
            final int length = in.readUnsignedShort();
            if (length < 4 || length > maximumLength || !sequences.add(seq)) {
                throw new IllegalArgumentException("invalid FEC entry");
            }
            entries[i] = new Entry(seq, length, null);
        }
        return entries;
    }

    static byte[] xor(List<byte[]> packets) {
        int max = 0;
        for (byte[] packet : packets) max = Math.max(max, packet.length);
        final byte[] parity = new byte[max];
        for (byte[] packet : packets) xorInto(parity, packet);
        return parity;
    }

    static byte[] recover(byte[] parity, List<byte[]> present, int missingLength) {
        final byte[] recovered = Arrays.copyOf(parity, missingLength);
        for (byte[] packet : present) xorInto(recovered, packet);
        return recovered;
    }

    static void xorInto(byte[] target, byte[] value) {
        for (int i = 0; i < Math.min(target.length, value.length); i++) target[i] ^= value[i];
    }

    static boolean sameEntries(Entry[] a, Entry[] b) {
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i].seq != b[i].seq || a[i].length != b[i].length) return false;
        }
        return true;
    }

    static final class Entry {
        final int seq;
        final int length;
        final byte[] data;

        Entry(int seq, int length, byte[] data) {
            this.seq = seq;
            this.length = length;
            this.data = data;
        }
    }

    static final class LegacyParity {
        final int id;
        final Entry[] entries;
        final byte[] parity;
        final long createdAt;

        LegacyParity(int id, Entry[] entries, byte[] parity, long createdAt) {
            this.id = id;
            this.entries = entries;
            this.parity = parity;
            this.createdAt = createdAt;
        }
    }

    static final class RsParity {
        final int id;
        final Entry[] entries;
        final int parityCount;
        final int parityIndex;
        final byte[] parity;
        final long createdAt;

        RsParity(int id, Entry[] entries, int parityCount, int parityIndex,
                 byte[] parity, long createdAt) {
            this.id = id;
            this.entries = entries;
            this.parityCount = parityCount;
            this.parityIndex = parityIndex;
            this.parity = parity;
            this.createdAt = createdAt;
        }
    }
}
