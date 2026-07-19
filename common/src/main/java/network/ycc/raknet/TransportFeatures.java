package network.ycc.raknet;

/** Negotiated extensions for the fork-specific RakNet protocol v12. */
public final class TransportFeatures {
    public static final int MAGIC = 0x4f4e5431; // "ONT1"
    public static final long FEC = 1L;
    public static final long PLPMTUD = 1L << 1;
    public static final long DYNAMIC_FEC = 1L << 2;
    public static final long REED_SOLOMON_FEC = 1L << 3;
    public static final long DPLPMTUD_STATE_MACHINE = 1L << 4;
    public static final long MODEL_CONGESTION_CONTROL = 1L << 5;
    public static final long SUPPORTED = FEC | PLPMTUD | DYNAMIC_FEC | REED_SOLOMON_FEC
            | DPLPMTUD_STATE_MACHINE | MODEL_CONGESTION_CONTROL;

    private TransportFeatures() {}

    /**
     * Intersects both peers' capabilities and removes dependent features whose
     * required base transport was not negotiated.
     */
    public static long negotiate(long local, long remote) {
        long negotiated = local & remote & SUPPORTED;
        if ((negotiated & FEC) == 0) {
            negotiated &= ~(DYNAMIC_FEC | REED_SOLOMON_FEC);
        }
        if ((negotiated & PLPMTUD) == 0) {
            negotiated &= ~DPLPMTUD_STATE_MACHINE;
        }
        return negotiated;
    }
}
