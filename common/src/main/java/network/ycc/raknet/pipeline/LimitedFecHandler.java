package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultCodec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
    private int groupId;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!enabled(ctx) || !(msg instanceof ByteBuf)) {
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
                pending.add(readParity(buf));
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
    }

    private boolean enabled(ChannelHandlerContext ctx) {
        return RakNet.config(ctx).getProtocolVersion() >= ADAPTIVE_PROTOCOL_VERSION;
    }

    private static boolean isFrameSet(ByteBuf buf) {
        if (buf.readableBytes() < 4) return false;
        final int id = buf.getUnsignedByte(buf.readerIndex());
        return id >= DefaultCodec.FRAME_DATA_START && id <= DefaultCodec.FRAME_DATA_END;
    }

    private ByteBuf createParity(ChannelHandlerContext ctx) {
        int max = 0;
        for (Entry entry : outbound) max = Math.max(max, entry.length);
        final byte[] parity = new byte[max];
        for (Entry entry : outbound) for (int i = 0; i < entry.length; i++) parity[i] ^= entry.data[i];
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
        for (int i = 0; i < count; i++) entries[i] = new Entry(in.readUnsignedMediumLE(), in.readUnsignedShort(), null);
        final int size = in.readUnsignedShort();
        if (size > MAX_PROTECTED_BYTES || in.readableBytes() != size) throw new IllegalArgumentException("Invalid FEC parity size");
        final byte[] parity = new byte[size];
        in.readBytes(parity);
        return new Parity(id, entries, parity);
    }

    private void tryRecover(ChannelHandlerContext ctx) {
        final Iterator<Parity> iterator = pending.iterator();
        while (iterator.hasNext()) {
            final Parity group = iterator.next();
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
                ctx.fireChannelRead(ctx.alloc().ioBuffer(recovered.length).writeBytes(recovered));
            }
        }
        while (pending.size() > 64) pending.remove(0);
    }

    private static byte[] copy(ByteBuf buf) {
        final byte[] out = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), out);
        return out;
    }

    private static final class Entry {
        final int seq, length; final byte[] data;
        Entry(int seq, int length, byte[] data) { this.seq = seq; this.length = length; this.data = data; }
    }
    private static final class Parity {
        final int id; final Entry[] entries; final byte[] parity;
        Parity(int id, Entry[] entries, byte[] parity) { this.id = id; this.entries = entries; this.parity = parity; }
    }
}
