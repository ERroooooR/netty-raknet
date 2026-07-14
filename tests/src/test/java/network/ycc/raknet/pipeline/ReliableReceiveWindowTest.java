package network.ycc.raknet.pipeline;

import network.ycc.raknet.utils.UINT;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReliableReceiveWindowTest {

    @Test
    void rejectsDuplicatesButAcceptsReordering() {
        final ReliableReceiveWindow window = new ReliableReceiveWindow(8);

        assertTrue(window.accept(10));
        assertTrue(window.accept(12));
        assertTrue(window.accept(11));
        assertFalse(window.accept(10));
        assertFalse(window.accept(11));
        assertFalse(window.accept(12));
    }

    @Test
    void rejectsFramesOlderThanTheWindow() {
        final ReliableReceiveWindow window = new ReliableReceiveWindow(8);

        assertTrue(window.accept(100));
        assertTrue(window.accept(108));
        assertFalse(window.accept(100));
        assertTrue(window.accept(107));
    }

    @Test
    void handlesUint24Wraparound() {
        final ReliableReceiveWindow window = new ReliableReceiveWindow(8);
        final int beforeWrap = UINT.B3.MAX_VALUE - 1;

        assertTrue(window.accept(beforeWrap));
        assertTrue(window.accept(UINT.B3.MAX_VALUE));
        assertTrue(window.accept(0));
        assertTrue(window.accept(1));
        assertFalse(window.accept(UINT.B3.MAX_VALUE));
        assertFalse(window.accept(0));
    }

    @Test
    void rejectsInvalidIndices() {
        final ReliableReceiveWindow window = new ReliableReceiveWindow(8);

        assertFalse(window.accept(-1));
        assertFalse(window.accept(UINT.B3.MAX_VALUE + 1));
    }
}
