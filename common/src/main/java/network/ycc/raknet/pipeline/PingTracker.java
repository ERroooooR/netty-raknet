package network.ycc.raknet.pipeline;

import java.util.ArrayDeque;

/** Tracks locally issued ping tokens so peers cannot forge RTT samples. */
public final class PingTracker {
    private static final int MAX_PENDING = 32;
    private final ArrayDeque<Long> pending = new ArrayDeque<>(MAX_PENDING);

    public void issued(long token) {
        if (pending.size() == MAX_PENDING) pending.removeFirst();
        pending.addLast(token);
    }

    public boolean acknowledge(long token) {
        return pending.removeFirstOccurrence(token);
    }

    public void clear() {
        pending.clear();
    }
}
