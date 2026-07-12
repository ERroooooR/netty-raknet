package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.TransportFeatures;
import network.ycc.raknet.config.DefaultCodec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Optional one-loss XOR recovery for small FrameSets. Wire format is restricted to protocol v12+. */
public final class LimitedFecHandler extends ChannelDuplexHandler {
    public static final String NAME = "rn-limited-fec";
    public static final int ADAPTIVE_PROTOCOL_VERSION = 12;
    private static final int FEC_PACKET_ID = 0x1e;
    private static final int GROUP_SIZE = 4;
    private static final int MAX_PROTECTED_BYTES = 512;
    private static final int MAX_CACHE = 256;

    private final List<Entry> outbound = new ArrayList<>(GROUP_SIZE);
    private final Int2ObjectLinkedOpenHashMap<byte[]> received = new Int2ObjectLinkedOpenHashMap<>();
    private final List<Parity> pending = new ArrayList<>();
    private final Set<Integer> recentGroups = new LinkedHashSet<>();
    private int groupId;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!enabled(ctx) || !fecUseful(ctx) || !(msg instanceof ByteBuf)) {
            outbound.clear();
            ctx.write(msg, promise);
            return;
        }
        final ByteBuf buf = (ByteBuf) msg;
        if (isFrameSet(buf) && buf.readableBytes() <= MAX_PROTECTED_BYTES) {
            final byte[] bytes = copy(buf);
            outbound.add(new Entry(buf.getUnsignedMediumLE(buf.readerIndex() + 1), bytes.length, bytes));
        }
        ctx.write(msg, promise);
        if (outbound.size() == GROUP_SIZE) {
            ctx.write(createParity(ctx), ctx.voidPromise());
            outbound.clear();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!enabled(ctx) || !(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }
        final ByteBuf buf = (ByteBuf) msg;
        if (buf.isReadable() && buf.getUnsignedByte(buf.readerIndex()) == FEC_PACKET_ID) {
            try {
                final Parity parity = readParity(buf);
                if (recentGroups.add(parity.id)) pending.add(parity);
                while (recentGroups.size() > 128) {
                    final Iterator<Integer> groups = recentGroups.iterator();
                    groups.next();
                    groups.remove();
                }
                tryRecover(ctx);
            } finally {
                ReferenceCountUtil.release(msg);
            }
            return;
        }
        if (isFrameSet(buf)) {
            received.put(buf.getUnsignedMediumLE(buf.readerIndex() + 1), copy(buf));
            while (received.size() > MAX_CACHE) received.removeFirst();
            tryRecover(ctx);
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        outbound.clear();
        received.clear();
        pending.clear();
        recentGroups.clear();
    }

    private boolean enabled(ChannelHandlerContext ctx) {
        final Long features = ctx.channel().attr(RakNet.TRANSPORT_FEATURES).get();
        return RakNet.config(ctx).getProtocolVersion() >= ADAPTIVE_PROTOCOL_VERSION
                && features != null && (features & TransportFeatures.FEC) != 0;
    }

    private static boolean isFrameSet(ByteBuf buf) {
        if (buf.readableBytes() < 4) return false;
        final int id = buf.getUnsignedByte(buf.readerIndex());
        return id >= DefaultCodec.FRAME_DATA_START && id <= DefaultCodec.FRAME_DATA_END;
    }

    private ByteBuf createParity(ChannelHandlerContext ctx) {
        final List<byte[]> packets = new ArrayList<>(outbound.size());
        for (Entry entry : outbound) packets.add(entry.data);
        final byte[] parity = xor(packets);
        final int max = parity.length;
        final ByteBuf out = ctx.alloc().ioBuffer(8 + GROUP_SIZE * 5 + max);
        out.writeByte(FEC_PACKET_ID).writeInt(groupId++).writeByte(GROUP_SIZE);
        for (Entry entry : outbound) out.writeMediumLE(entry.seq).writeShort(entry.length);
        out.writeShort(max).writeBytes(parity);
        return out;
    }

    private static Parity readParity(ByteBuf in) {
        in.skipBytes(1);
        final int id = in.readInt();
        final int count = in.readUnsignedByte();
        if (count != GROUP_SIZE) throw new IllegalArgumentException("Invalid FEC group size");
        final Entry[] entries = new Entry[count];
        final java.util.HashSet<Integer> sequences = new java.util.HashSet<>();
        for (int i = 0; i < count; i++) {
            final int seq = in.readUnsignedMediumLE();
            final int length = in.readUnsignedShort();
            if (length < 4 || length > MAX_PROTECTED_BYTES || !sequences.add(seq)) {
                throw new IllegalArgumentException("Invalid FEC entry");
            }
            entries[i] = new Entry(seq, length, null);
        }
        final int size = in.readUnsignedShort();
        if (size > MAX_PROTECTED_BYTES || in.readableBytes() != size) throw new IllegalArgumentException("Invalid FEC parity size");
        for (Entry entry : entries) if (entry.length > size) throw new IllegalArgumentException("FEC entry exceeds parity");
        final byte[] parity = new byte[size];
        in.readBytes(parity);
        return new Parity(id, entries, parity, System.nanoTime());
    }

    private void tryRecover(ChannelHandlerContext ctx) {
        final Iterator<Parity> iterator = pending.iterator();
        while (iterator.hasNext()) {
            final Parity group = iterator.next();
            if (System.nanoTime() - group.createdAt > TimeUnit.SECONDS.toNanos(5)) {
                iterator.remove();
                continue;
            }
            Entry missing = null;
            int missingCount = 0;
            final byte[] data = Arrays.copyOf(group.parity, group.parity.length);
            for (Entry entry : group.entries) {
                final byte[] value = received.get(entry.seq);
                if (value == null) {
                    missing = entry;
                    missingCount++;
                } else {
                    for (int i = 0; i < value.length; i++) data[i] ^= value[i];
                }
            }
            if (missingCount == 0) {
                iterator.remove();
            } else if (missingCount == 1) {
                final byte[] recovered = Arrays.copyOf(data, missing.length);
                received.put(missing.seq, recovered);
                iterator.remove();
                RakNet.config(ctx).getMetrics().fecRecovered(1);
                ctx.fireChannelRead(ctx.alloc().ioBuffer(recovered.length).writeBytes(recovered));
            }
        }
        while (pending.size() > 64) pending.remove(0);
    }

    private boolean fecUseful(ChannelHandlerContext ctx) {
        final ReliabilityHandler reliability = ctx.pipeline().get(ReliabilityHandler.class);
        return reliability != null && reliability.adaptiveController().shouldUseFec();
    }

    private static byte[] copy(ByteBuf buf) {
        final byte[] out = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), out);
        return out;
    }

    static byte[] xor(List<byte[]> packets) {
        int max = 0;
        for (byte[] packet : packets) max = Math.max(max, packet.length);
        final byte[] parity = new byte[max];
        for (byte[] packet : packets) for (int i = 0; i < packet.length; i++) parity[i] ^= packet[i];
        return parity;
    }

    static byte[] recover(byte[] parity, List<byte[]> present, int missingLength) {
        final byte[] recovered = Arrays.copyOf(parity, missingLength);
        for (byte[] packet : present) {
            for (int i = 0; i < Math.min(packet.length, recovered.length); i++) recovered[i] ^= packet[i];
        }
        return recovered;
    }

    private static final class Entry {
        final int seq, length; final byte[] data;
        Entry(int seq, int length, byte[] data) { this.seq = seq; this.length = length; this.data = data; }
    }
    private static final class Parity {
        final int id; final Entry[] entries; final byte[] parity; final long createdAt;
        Parity(int id, Entry[] entries, byte[] parity, long createdAt) {
            this.id = id; this.entries = entries; this.parity = parity; this.createdAt = createdAt;
        }
    }
}
