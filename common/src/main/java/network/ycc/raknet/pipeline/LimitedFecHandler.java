package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.TransportFeatures;
import network.ycc.raknet.config.DefaultCodec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Negotiated bounded XOR/Reed-Solomon recovery for RakNet FrameSets. */
public final class LimitedFecHandler extends ChannelDuplexHandler {
    public static final String NAME = "rn-limited-fec";
    public static final int ADAPTIVE_PROTOCOL_VERSION = 12;
    private static final int LEGACY_FEC_PACKET_ID = 0x1e;
    private static final int RS_FEC_PACKET_ID = 0x22;
    private static final int FEC_FEEDBACK_PACKET_ID = 0x23;
    private static final int MIN_GROUP_SIZE = 4;
    private static final int MAX_GROUP_SIZE = 12;
    private static final int LEGACY_MAX_PROTECTED_BYTES = 512;
    private static final int MAX_CACHE = 512;
    private static final int MAX_PENDING_GROUPS = 64;
    private static final int TARGETED_WINDOW_SIZE = 4;
    private static final int TARGETED_ROLLING_CACHE = 8;
    private static final long NORMAL_FEC_FEEDBACK_WINDOW = 64;
    private static final long NORMAL_FEC_SUPPRESSION_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final List<FecPacketCodec.Entry> outbound = new ArrayList<>(MAX_GROUP_SIZE);
    private final List<FecPacketCodec.Entry> rollingOutbound = new ArrayList<>(TARGETED_ROLLING_CACHE);
    private final Int2ObjectLinkedOpenHashMap<byte[]> received = new Int2ObjectLinkedOpenHashMap<>();
    private final List<FecPacketCodec.LegacyParity> legacyPending = new ArrayList<>();
    private final Int2ObjectLinkedOpenHashMap<RsGroup> rsPending = new Int2ObjectLinkedOpenHashMap<>();
    private final Set<Integer> recentGroups = new LinkedHashSet<>();
    private final Set<Integer> targetedGroups = new LinkedHashSet<>();
    private int groupId;
    private int outboundDataShards;
    private int outboundParityShards;
    private long feedbackGroups;
    private long feedbackRecovered;
    private long normalFecSuppressedUntilNanos;
    private ScheduledFuture<?> cleanupTask;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        final boolean enabled = enabled(ctx);
        if (!enabled || !(msg instanceof ByteBuf)) {
            resetOutbound();
            if (!enabled) rollingOutbound.clear();
            ctx.write(msg, promise);
            return;
        }
        final boolean reedSolomon = reedSolomonEnabled(ctx);
        final ReliabilityHandler reliability = ctx.pipeline().get(ReliabilityHandler.class);
        final long now = System.nanoTime();
        final boolean normalFec = reliability != null && reliability.adaptiveController().shouldUseFec()
                && normalFecAllowed(ctx, now);
        final AdaptiveTransportController.FecParameters parameters = normalFec
                ? fecParameters(ctx, reedSolomon)
                : new AdaptiveTransportController.FecParameters(TARGETED_WINDOW_SIZE, 1, 0.0D);
        if (normalFec) {
            if (outboundDataShards != 0 && (outboundDataShards != parameters.dataShards
                    || outboundParityShards != parameters.parityShards)) outbound.clear();
            outboundDataShards = parameters.dataShards;
            outboundParityShards = parameters.parityShards;
        } else {
            resetOutbound();
        }

        final ByteBuf buf = (ByteBuf) msg;
        final int payloadLimit = reedSolomon
                ? Math.max(256, RakNet.config(ctx).getMTU() - (12 + parameters.dataShards * 5))
                : LEGACY_MAX_PROTECTED_BYTES;
        FecPacketCodec.Entry current = null;
        if (isFrameSet(buf) && buf.readableBytes() <= payloadLimit) {
            final byte[] bytes = copy(buf);
            current = new FecPacketCodec.Entry(
                    buf.getUnsignedMediumLE(buf.readerIndex() + 1), bytes.length, bytes);
            if (reedSolomon) rememberRolling(current);
            if (normalFec) outbound.add(current);
        }
        ctx.write(msg, promise);
        boolean wroteNormalParity = false;
        if (normalFec && outbound.size() == parameters.dataShards) {
            if (reedSolomon) writeReedSolomonParity(ctx, parameters.parityShards);
            else writeLegacyParity(ctx);
            outbound.clear();
            wroteNormalParity = true;
        }
        if (!wroteNormalParity && reedSolomon && current != null && reliability != null) {
            final int targetSequence = selectTargetedSequence(reliability, now);
            if (targetSequence >= 0) writeTargetedParity(ctx, reliability, targetSequence, now);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }
        final ByteBuf buf = (ByteBuf) msg;
        final int id = buf.isReadable() ? buf.getUnsignedByte(buf.readerIndex()) : -1;
        if (!enabled(ctx)) {
            if (id == LEGACY_FEC_PACKET_ID || id == RS_FEC_PACKET_ID || id == FEC_FEEDBACK_PACKET_ID) {
                ReferenceCountUtil.release(msg);
                return;
            }
            ctx.fireChannelRead(msg);
            return;
        }
        if (id == FEC_FEEDBACK_PACKET_ID) {
            try {
                buf.skipBytes(1);
                final int feedbackGroupId = buf.readInt();
                final int recovered = buf.readUnsignedByte();
                if (recovered > 2 || buf.isReadable()) throw new IllegalArgumentException("invalid FEC feedback");
                if (targetedGroups.remove(feedbackGroupId)) {
                    if (recovered > 0) RakNet.config(ctx).getMetrics().targetedFecRecovered(recovered);
                } else {
                    feedbackGroups++;
                    feedbackRecovered += recovered;
                    if (feedbackGroups >= 128) {
                        feedbackGroups /= 2;
                        feedbackRecovered /= 2;
                    }
                }
            } catch (RuntimeException ignored) {
                // Untrusted extension packet.
            } finally {
                ReferenceCountUtil.release(msg);
            }
            return;
        }
        if (id == LEGACY_FEC_PACKET_ID || id == RS_FEC_PACKET_ID) {
            try {
                if (id == LEGACY_FEC_PACKET_ID) {
                    final FecPacketCodec.LegacyParity parity = FecPacketCodec.readLegacyParity(buf);
                    if (!recentGroups.contains(parity.id)) {
                        legacyPending.add(parity);
                        while (legacyPending.size() > MAX_PENDING_GROUPS) legacyPending.remove(0);
                    }
                } else if (reedSolomonEnabled(ctx)) {
                    mergeReedSolomonParity(FecPacketCodec.readReedSolomonParity(buf));
                }
                tryRecover(ctx);
                maintainCleanupTask(ctx);
            } catch (RuntimeException ignored) {
                // Malformed or inconsistent FEC packet from wire.
            } finally {
                ReferenceCountUtil.release(msg);
            }
            return;
        }
        if (isFrameSet(buf)) {
            received.put(buf.getUnsignedMediumLE(buf.readerIndex() + 1), copy(buf));
            while (received.size() > MAX_CACHE) received.removeFirst();
            tryRecover(ctx);
            maintainCleanupTask(ctx);
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        cancelCleanupTask();
        resetOutbound();
        rollingOutbound.clear();
        received.clear();
        legacyPending.clear();
        rsPending.clear();
        recentGroups.clear();
        targetedGroups.clear();
    }

    private void writeLegacyParity(ChannelHandlerContext ctx) {
        final List<byte[]> packets = outboundData();
        final byte[] parity = FecPacketCodec.xor(packets);
        final ByteBuf out = ctx.alloc().ioBuffer(8 + outbound.size() * 5 + parity.length);
        out.writeByte(LEGACY_FEC_PACKET_ID).writeInt(groupId++).writeByte(outbound.size());
        writeEntries(out);
        out.writeShort(parity.length).writeBytes(parity);
        RakNet.config(ctx).getMetrics().fecParity(1, out.readableBytes());
        ctx.write(out, ctx.voidPromise());
    }

    private void writeReedSolomonParity(ChannelHandlerContext ctx, int parityShards) {
        final int id = groupId++;
        final byte[][] parity = ReedSolomonCodec.encode(outboundData(), parityShards);
        int bytes = 0;
        for (int p = 0; p < parity.length; p++) {
            final ByteBuf out = ctx.alloc().ioBuffer(10 + outbound.size() * 5 + parity[p].length);
            out.writeByte(RS_FEC_PACKET_ID).writeInt(id).writeByte(outbound.size())
                    .writeByte(parity.length).writeByte(p);
            writeEntries(out);
            out.writeShort(parity[p].length).writeBytes(parity[p]);
            bytes += out.readableBytes();
            ctx.write(out, ctx.voidPromise());
        }
        RakNet.config(ctx).getMetrics().fecParity(parity.length, bytes);
        final double recoveryRatio = feedbackGroups == 0 ? 0D : feedbackRecovered / (double) feedbackGroups;
        RakNet.config(ctx).getMetrics().fecBudget(outbound.size(), parity.length, recoveryRatio);
    }

    private void writeEntries(ByteBuf out) {
        for (FecPacketCodec.Entry entry : outbound) {
            out.writeMediumLE(entry.seq).writeShort(entry.length);
        }
    }

    private static void writeEntries(ByteBuf out, List<FecPacketCodec.Entry> entries) {
        for (FecPacketCodec.Entry entry : entries) {
            out.writeMediumLE(entry.seq).writeShort(entry.length);
        }
    }

    private void rememberRolling(FecPacketCodec.Entry entry) {
        rollingOutbound.removeIf(existing -> existing.seq == entry.seq);
        rollingOutbound.add(entry);
        while (rollingOutbound.size() > TARGETED_ROLLING_CACHE) rollingOutbound.remove(0);
    }

    private int selectTargetedSequence(ReliabilityHandler reliability, long now) {
        int sequence = -1;
        double maximumDebt = 0D;
        for (FecPacketCodec.Entry entry : rollingOutbound) {
            if (reliability.targetedFecChannel(entry.seq) < 0) continue;
            final double debt = reliability.recoveryDebtForSequence(entry.seq, now);
            if (debt >= 2D && reliability.isRemoteOrderedHolTarget(entry.seq, now)) {
                return entry.seq;
            }
            if (debt >= 2D && debt > maximumDebt) {
                maximumDebt = debt;
                sequence = entry.seq;
            }
        }
        if (sequence < 0) RakNet.config(reliability.ctx).getMetrics().recoveryDebt(0D, -1);
        return sequence;
    }

    private void writeTargetedParity(ChannelHandlerContext ctx, ReliabilityHandler reliability,
                                     int targetSequence, long now) {
        FecPacketCodec.Entry target = null;
        for (FecPacketCodec.Entry entry : rollingOutbound) {
            if (entry.seq == targetSequence) target = entry;
        }
        if (target == null) return;
        final List<FecPacketCodec.Entry> group = selectTargetedGroup(
                rollingOutbound, targetSequence);
        if (group.size() < TARGETED_WINDOW_SIZE) return;
        final List<byte[]> data = new ArrayList<>(group.size());
        for (FecPacketCodec.Entry entry : group) data.add(entry.data);
        final byte[] parity = ReedSolomonCodec.encode(data, 1)[0];
        final int packetBytes = 10 + group.size() * 5 + parity.length;
        if (!reliability.tryAcquireTargetedFecBudget(targetSequence, packetBytes, now)) return;

        final int id = groupId++;
        targetedGroups.add(id);
        while (targetedGroups.size() > MAX_PENDING_GROUPS) {
            final Iterator<Integer> iterator = targetedGroups.iterator();
            iterator.next();
            iterator.remove();
        }
        final ByteBuf out = ctx.alloc().ioBuffer(packetBytes);
        out.writeByte(RS_FEC_PACKET_ID).writeInt(id).writeByte(group.size())
                .writeByte(1).writeByte(0);
        writeEntries(out, group);
        out.writeShort(parity.length).writeBytes(parity);
        final int bytes = out.readableBytes();
        ctx.write(out, ctx.voidPromise());
        RakNet.config(ctx).getMetrics().fecParity(1, bytes);
        RakNet.config(ctx).getMetrics().targetedFecRepair(
                reliability.targetedFecChannel(targetSequence), bytes);
    }

    static List<FecPacketCodec.Entry> selectTargetedGroup(
            List<FecPacketCodec.Entry> rolling, int targetSequence) {
        FecPacketCodec.Entry target = null;
        for (FecPacketCodec.Entry entry : rolling) {
            if (entry.seq == targetSequence) target = entry;
        }
        final List<FecPacketCodec.Entry> group = new ArrayList<>(TARGETED_WINDOW_SIZE);
        if (target == null) return group;
        group.add(target);
        for (int i = rolling.size() - 1; i >= 0 && group.size() < TARGETED_WINDOW_SIZE; i--) {
            final FecPacketCodec.Entry entry = rolling.get(i);
            if (entry.seq != targetSequence) group.add(entry);
        }
        return group;
    }

    private List<byte[]> outboundData() {
        final List<byte[]> packets = new ArrayList<>(outbound.size());
        for (FecPacketCodec.Entry entry : outbound) packets.add(entry.data);
        return packets;
    }

    private void mergeReedSolomonParity(FecPacketCodec.RsParity shard) {
        if (recentGroups.contains(shard.id)) return;
        RsGroup group = rsPending.get(shard.id);
        if (group == null) {
            group = new RsGroup(shard.id, shard.entries, new byte[shard.parityCount][], shard.createdAt);
            rsPending.put(shard.id, group);
        } else if (!FecPacketCodec.sameEntries(group.entries, shard.entries)
                || group.parity.length != shard.parityCount) {
            throw new IllegalArgumentException("inconsistent Reed-Solomon group");
        }
        if (group.parity[shard.parityIndex] == null) group.parity[shard.parityIndex] = shard.parity;
        while (rsPending.size() > 64) rsPending.removeFirst();
    }

    private void tryRecover(ChannelHandlerContext ctx) {
        final long now = System.nanoTime();
        final Iterator<FecPacketCodec.LegacyParity> legacy = legacyPending.iterator();
        while (legacy.hasNext()) {
            final FecPacketCodec.LegacyParity group = legacy.next();
            if (expired(ctx, group.createdAt)) { legacy.remove(); continue; }
            FecPacketCodec.Entry missing = null;
            int missingCount = 0;
            boolean inconsistent = false;
            final byte[] data = Arrays.copyOf(group.parity, group.parity.length);
            for (FecPacketCodec.Entry entry : group.entries) {
                final byte[] value = received.get(entry.seq);
                if (value == null) { missing = entry; missingCount++; }
                else if (value.length != entry.length) { inconsistent = true; break; }
                else FecPacketCodec.xorInto(data, value);
            }
            if (inconsistent) {
                legacy.remove(); rememberGroup(group.id); continue;
            }
            if (missingCount == 0) {
                legacy.remove(); rememberGroup(group.id);
            } else if (missingCount == 1) {
                final byte[] recovered = Arrays.copyOf(data, missing.length);
                deliverRecovered(ctx, missing.seq, recovered);
                legacy.remove(); rememberGroup(group.id);
            }
        }

        final Iterator<RsGroup> rs = rsPending.values().iterator();
        while (rs.hasNext()) {
            final RsGroup group = rs.next();
            if (expired(ctx, group.createdAt)) { rs.remove(); rememberGroup(group.id); continue; }
            final byte[][] data = new byte[group.entries.length][];
            final int[] lengths = new int[group.entries.length];
            int missing = 0;
            boolean inconsistent = false;
            for (int i = 0; i < group.entries.length; i++) {
                data[i] = received.get(group.entries[i].seq);
                lengths[i] = group.entries[i].length;
                if (data[i] == null) missing++;
                else if (data[i].length != lengths[i]) { inconsistent = true; break; }
            }
            if (inconsistent) {
                rs.remove(); rememberGroup(group.id); continue;
            }
            if (missing == 0) {
                sendFeedback(ctx, group.id, 0);
                rs.remove(); rememberGroup(group.id);
                continue;
            }
            if (available(group.parity) < missing) continue;
            final byte[][] recovered = ReedSolomonCodec.recover(data, lengths, group.parity);
            if (recovered == null) continue;
            for (int i = 0, r = 0; i < data.length; i++) {
                if (data[i] == null) deliverRecovered(ctx, group.entries[i].seq, recovered[r++]);
            }
            sendFeedback(ctx, group.id, missing);
            rs.remove(); rememberGroup(group.id);
        }
    }

    private void maintainCleanupTask(ChannelHandlerContext ctx) {
        if (legacyPending.isEmpty() && rsPending.isEmpty()) {
            cancelCleanupTask();
            return;
        }
        if (cleanupTask != null && !cleanupTask.isDone()) return;
        cleanupTask = ctx.executor().scheduleAtFixedRate(() -> {
            try {
                tryRecover(ctx);
                if (legacyPending.isEmpty() && rsPending.isEmpty()) cancelCleanupTask();
            } catch (RuntimeException ignored) {
                // A malformed cached group must not cancel periodic cleanup permanently.
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void cancelCleanupTask() {
        if (cleanupTask != null) cleanupTask.cancel(false);
        cleanupTask = null;
    }

    private boolean expired(ChannelHandlerContext ctx, long createdAt) {
        if (System.nanoTime() - createdAt <= TimeUnit.SECONDS.toNanos(5)) return false;
        RakNet.config(ctx).getMetrics().fecExpired(1);
        return true;
    }

    private void deliverRecovered(ChannelHandlerContext ctx, int seq, byte[] recovered) {
        received.put(seq, recovered);
        RakNet.config(ctx).getMetrics().fecRecovered(1);
        ctx.fireChannelRead(ctx.alloc().ioBuffer(recovered.length).writeBytes(recovered));
    }

    private void sendFeedback(ChannelHandlerContext ctx, int id, int recovered) {
        final ByteBuf feedback = ctx.alloc().ioBuffer(6, 6);
        feedback.writeByte(FEC_FEEDBACK_PACKET_ID).writeInt(id).writeByte(recovered);
        ctx.writeAndFlush(feedback, ctx.voidPromise());
    }

    private AdaptiveTransportController.FecParameters fecParameters(ChannelHandlerContext ctx, boolean rs) {
        final ReliabilityHandler reliability = ctx.pipeline().get(ReliabilityHandler.class);
        if (!rs || reliability == null) return new AdaptiveTransportController.FecParameters(fecGroupSize(ctx), 1, 0.20D);
        final AdaptiveTransportController.FecParameters base = reliability.adaptiveController().fecParameters();
        if (feedbackGroups >= 16 && feedbackRecovered == 0 && base.parityShards == 1) {
            return new AdaptiveTransportController.FecParameters(Math.min(MAX_GROUP_SIZE, base.dataShards + 2), 1, base.overheadBudget);
        }
        final double benefit = feedbackGroups == 0 ? 0D : feedbackRecovered / (double) feedbackGroups;
        if (feedbackGroups >= 16 && benefit > 0.15D && base.parityShards < 2
                && 2D / (base.dataShards + 2D) <= 0.20D) {
            return new AdaptiveTransportController.FecParameters(base.dataShards, 2, 0.20D);
        }
        return base;
    }

    private boolean normalFecAllowed(ChannelHandlerContext ctx, long now) {
        if (now < normalFecSuppressedUntilNanos) return false;
        if (!shouldSuppressNormalFec(feedbackGroups, feedbackRecovered)) return true;
        final double ratio = feedbackGroups == 0 ? 0D : feedbackRecovered / (double) feedbackGroups;
        normalFecSuppressedUntilNanos = now + NORMAL_FEC_SUPPRESSION_NANOS;
        feedbackGroups = 0;
        feedbackRecovered = 0;
        RakNet.config(ctx).getMetrics().fecBudget(0, 0, ratio);
        return false;
    }

    static boolean shouldSuppressNormalFec(long groups, long recovered) {
        return groups >= NORMAL_FEC_FEEDBACK_WINDOW && recovered * 100L < groups;
    }

    private boolean enabled(ChannelHandlerContext ctx) {
        final Long features = ctx.channel().attr(RakNet.TRANSPORT_FEATURES).get();
        return RakNet.config(ctx).isAdaptiveTransportEnabled()
                && RakNet.config(ctx).getProtocolVersion() >= ADAPTIVE_PROTOCOL_VERSION
                && features != null && (features & TransportFeatures.FEC) != 0;
    }

    private boolean reedSolomonEnabled(ChannelHandlerContext ctx) {
        final Long features = ctx.channel().attr(RakNet.TRANSPORT_FEATURES).get();
        return features != null && (features & TransportFeatures.REED_SOLOMON_FEC) != 0;
    }

    private int fecGroupSize(ChannelHandlerContext ctx) {
        final Long features = ctx.channel().attr(RakNet.TRANSPORT_FEATURES).get();
        if (features == null || (features & TransportFeatures.DYNAMIC_FEC) == 0) return MIN_GROUP_SIZE;
        final ReliabilityHandler reliability = ctx.pipeline().get(ReliabilityHandler.class);
        return reliability == null ? MIN_GROUP_SIZE : reliability.adaptiveController().fecGroupSize();
    }

    static int selectGroupSize(double lossRatio) {
        if (lossRatio < 0.03D) return 8;
        if (lossRatio < 0.06D) return 6;
        return 4;
    }

    private static boolean isFrameSet(ByteBuf buf) {
        if (buf.readableBytes() < 4) return false;
        final int id = buf.getUnsignedByte(buf.readerIndex());
        return id >= DefaultCodec.FRAME_DATA_START && id <= DefaultCodec.FRAME_DATA_END;
    }

    private void rememberGroup(int id) {
        recentGroups.add(id);
        while (recentGroups.size() > 256) {
            final Iterator<Integer> iterator = recentGroups.iterator(); iterator.next(); iterator.remove();
        }
    }

    private void resetOutbound() { outbound.clear(); outboundDataShards = outboundParityShards = 0; }
    private static int available(byte[][] values) { int n = 0; for (byte[] value : values) if (value != null) n++; return n; }
    private static byte[] copy(ByteBuf buf) { final byte[] out = new byte[buf.readableBytes()]; buf.getBytes(buf.readerIndex(), out); return out; }
    private static final class RsGroup {
        final int id; final FecPacketCodec.Entry[] entries; final byte[][] parity; final long createdAt;
        RsGroup(int id, FecPacketCodec.Entry[] entries, byte[][] parity, long createdAt) {
            this.id = id; this.entries = entries; this.parity = parity; this.createdAt = createdAt;
        }
    }
}
