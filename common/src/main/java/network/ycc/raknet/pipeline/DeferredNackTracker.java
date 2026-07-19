package network.ycc.raknet.pipeline;

import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.function.IntConsumer;

/** Owns the bounded reorder-grace deadlines independently of the Netty handler. */
final class DeferredNackTracker {
    private final Int2LongOpenHashMap deadlines = new Int2LongOpenHashMap();

    boolean defer(int sequenceId, long deadlineNanos) {
        if (deadlines.containsKey(sequenceId)) return false;
        deadlines.put(sequenceId, deadlineNanos);
        return true;
    }

    boolean cancel(int sequenceId) {
        return deadlines.remove(sequenceId) != 0L;
    }

    long drainDue(long nowNanos, IntConsumer consumer) {
        long nextDeadline = Long.MAX_VALUE;
        final ObjectIterator<Int2LongMap.Entry> iterator = deadlines.int2LongEntrySet().fastIterator();
        while (iterator.hasNext()) {
            final Int2LongMap.Entry entry = iterator.next();
            if (entry.getLongValue() <= nowNanos) {
                consumer.accept(entry.getIntKey());
                iterator.remove();
            } else {
                nextDeadline = Math.min(nextDeadline, entry.getLongValue());
            }
        }
        return nextDeadline == Long.MAX_VALUE ? -1L : Math.max(0L, nextDeadline - nowNanos);
    }

    void drainAll(IntConsumer consumer) {
        final ObjectIterator<Int2LongMap.Entry> iterator = deadlines.int2LongEntrySet().fastIterator();
        while (iterator.hasNext()) {
            consumer.accept(iterator.next().getIntKey());
            iterator.remove();
        }
    }

    void clear() {
        deadlines.clear();
    }
}
