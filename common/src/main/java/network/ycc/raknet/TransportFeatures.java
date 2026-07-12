package network.ycc.raknet;

/** Negotiated extensions for the fork-specific RakNet protocol v12. */
public final class TransportFeatures {
    public static final int MAGIC = 0x4f4e5431; // "ONT1"
    public static final long FEC = 1L;
    public static final long PLPMTUD = 1L << 1;
    public static final long SUPPORTED = FEC | PLPMTUD;

    private TransportFeatures() {}
}
