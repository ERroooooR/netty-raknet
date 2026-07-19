package network.ycc.raknet.pipeline;

import network.ycc.raknet.utils.UINT;

import java.util.Arrays;

/**
 * Sliding receive window for RakNet's 24-bit reliable message index.
 *
 * <p>FrameSet sequence numbers cannot be used for duplicate suppression because
 * retransmission assigns a new FrameSet sequence number while retaining each
 * frame's reliable index.</p>
 */
final class ReliableReceiveWindow {

    static final int DEFAULT_WINDOW_SIZE = 1 << 16;

    private final int[] slots;
    private final int mask;
    private boolean initialized;
    private int newestIndex;

    ReliableReceiveWindow() {
        this(DEFAULT_WINDOW_SIZE);
    }

    ReliableReceiveWindow(int windowSize) {
        if (windowSize <= 0 || Integer.bitCount(windowSize) != 1 || windowSize > UINT.B3.HALF_MAX) {
            throw new IllegalArgumentException("windowSize must be a power of two no larger than the uint24 half range");
        }
        slots = new int[windowSize];
        Arrays.fill(slots, -1);
        mask = windowSize - 1;
    }

    /** Returns true only for the first acceptable delivery of a reliable index. */
    boolean accept(int reliableIndex) {
        if (reliableIndex < 0 || reliableIndex > UINT.B3.MAX_VALUE) {
            return false;
        }
        if (!initialized) {
            initialized = true;
            newestIndex = reliableIndex;
            remember(reliableIndex);
            return true;
        }

        final int distance = UINT.B3.minusWrap(reliableIndex, newestIndex);
        if (distance > 0) {
            newestIndex = reliableIndex;
        } else if (distance == 0 || -(long) distance >= slots.length) {
            return false;
        }

        final int slot = reliableIndex & mask;
        if (slots[slot] == reliableIndex) {
            return false;
        }
        slots[slot] = reliableIndex;
        return true;
    }

    private void remember(int reliableIndex) {
        slots[reliableIndex & mask] = reliableIndex;
    }
}
