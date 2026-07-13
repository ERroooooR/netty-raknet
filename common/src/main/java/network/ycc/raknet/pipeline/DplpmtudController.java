package network.ycc.raknet.pipeline;

/** RFC 8899-style DPLPMTUD state and search engine. Event-loop confined. */
final class DplpmtudController {
    enum State { BASE, SEARCHING, SEARCH_COMPLETE, ERROR, DISABLED }

    static final int MIN_PLPMTU = 576;
    static final int BASE_PLPMTU = 1200;
    static final int MIN_SEARCH_GAIN = 16;
    static final int MAX_PROBES = 3;
    static final long PMTU_RAISE_NANOS = 10L * 60L * 1_000_000_000L;

    private final int baseMtu;
    private final int maximumMtu;
    private int confirmedMtu;
    private int lowerBound;
    private int upperBound;
    private int probedMtu;
    private int probeCount;
    private long searchCompletedAt;
    private State state;

    DplpmtudController(int initialMtu, int configuredMaximumMtu) {
        confirmedMtu = Math.max(MIN_PLPMTU, initialMtu);
        baseMtu = Math.min(confirmedMtu, BASE_PLPMTU);
        maximumMtu = Math.max(confirmedMtu, configuredMaximumMtu);
        lowerBound = confirmedMtu;
        upperBound = maximumMtu;
        state = lowerBound < upperBound ? State.SEARCHING : State.SEARCH_COMPLETE;
    }

    int nextProbe(long now) {
        if (state == State.DISABLED) return -1;
        if (probedMtu != 0 && probeCount < MAX_PROBES) return probedMtu;
        if (state == State.SEARCH_COMPLETE) {
            if (now - searchCompletedAt < PMTU_RAISE_NANOS || confirmedMtu >= maximumMtu) return -1;
            lowerBound = confirmedMtu;
            upperBound = maximumMtu;
            state = State.SEARCHING;
        }
        if (state == State.BASE) return baseMtu;
        if (state == State.ERROR) return MIN_PLPMTU;
        if (upperBound - lowerBound < MIN_SEARCH_GAIN) {
            complete(now);
            return -1;
        }
        int candidate = lowerBound + ((upperBound - lowerBound + 1) / 2);
        // Stable 8-byte alignment avoids testing every size and makes captures reproducible.
        candidate = Math.min(upperBound, Math.max(lowerBound + MIN_SEARCH_GAIN, candidate & ~7));
        return candidate;
    }

    void onProbeSent(int mtu) {
        if (mtu != probedMtu) probeCount = 0;
        probedMtu = mtu;
    }

    void onProbeAcknowledged(int mtu, long now) {
        if (mtu != probedMtu && mtu < confirmedMtu) return;
        probeCount = 0;
        probedMtu = 0;
        if (state == State.BASE || state == State.ERROR) {
            confirmedMtu = Math.max(MIN_PLPMTU, mtu);
            lowerBound = confirmedMtu;
            state = State.SEARCHING;
            return;
        }
        if (mtu > confirmedMtu) confirmedMtu = Math.min(mtu, maximumMtu);
        lowerBound = Math.max(lowerBound, confirmedMtu);
        if (confirmedMtu >= maximumMtu) complete(now);
    }

    void onProbeTimeout(int mtu, long now) {
        if (mtu != probedMtu) return;
        if (++probeCount < MAX_PROBES) return;
        probeCount = 0;
        probedMtu = 0;
        if (state == State.BASE || state == State.ERROR) {
            if (state == State.ERROR) state = State.DISABLED;
            else state = State.ERROR;
            confirmedMtu = Math.max(MIN_PLPMTU, Math.min(confirmedMtu, baseMtu));
            return;
        }
        upperBound = Math.max(lowerBound, mtu - 1);
        if (upperBound - lowerBound < MIN_SEARCH_GAIN) complete(now);
    }

    /** A validated PTB is a search bound, not proof that its advertised MTU works. */
    void onPacketTooBig(int reportedMtu, int triggeringDatagramSize) {
        if (triggeringDatagramSize <= 0 || reportedMtu < MIN_PLPMTU
                || reportedMtu >= triggeringDatagramSize) return;
        upperBound = Math.min(upperBound, reportedMtu);
        probedMtu = 0;
        probeCount = 0;
        if (reportedMtu < confirmedMtu) {
            confirmedMtu = Math.max(MIN_PLPMTU, reportedMtu);
            lowerBound = confirmedMtu;
            state = reportedMtu < baseMtu ? State.ERROR : State.BASE;
        } else if (upperBound - lowerBound < MIN_SEARCH_GAIN) {
            state = State.SEARCH_COMPLETE;
        }
    }

    void onLocalMessageTooLong(int triggeringDatagramSize) {
        if (triggeringDatagramSize <= confirmedMtu) return;
        upperBound = Math.max(lowerBound, Math.min(upperBound, triggeringDatagramSize - 1));
        probedMtu = 0;
        probeCount = 0;
        if (upperBound - lowerBound < MIN_SEARCH_GAIN) state = State.SEARCH_COMPLETE;
    }

    void onBlackHole() {
        confirmedMtu = baseMtu;
        lowerBound = confirmedMtu;
        upperBound = Math.max(lowerBound, upperBound);
        probedMtu = 0;
        probeCount = 0;
        state = State.BASE;
    }

    private void complete(long now) {
        state = State.SEARCH_COMPLETE;
        searchCompletedAt = now;
        probedMtu = 0;
        probeCount = 0;
    }

    State state() { return state; }
    int confirmedMtu() { return confirmedMtu; }
    int probedMtu() { return probedMtu; }
    int maximumMtu() { return maximumMtu; }
    int probeCount() { return probeCount; }
}
