package network.ycc.raknet;

/**
 * Validated network feedback supplied by a native transport or socket-error adapter.
 * Standard Java datagram channels do not expose the Linux error queue, so integrations
 * can translate a validated ICMP Packet Too Big or an ECN-CE indication into this event.
 */
public final class TransportFeedbackEvent {
    public enum Type { PACKET_TOO_BIG, ECN_CE }

    private final Type type;
    private final int mtu;

    private TransportFeedbackEvent(Type type, int mtu) {
        this.type = type;
        this.mtu = mtu;
    }

    public static TransportFeedbackEvent packetTooBig(int mtu) {
        if (mtu < 0) throw new IllegalArgumentException("mtu must be >= 0");
        return new TransportFeedbackEvent(Type.PACKET_TOO_BIG, mtu);
    }

    public static TransportFeedbackEvent ecnCe() {
        return new TransportFeedbackEvent(Type.ECN_CE, 0);
    }

    public Type getType() { return type; }
    public int getMtu() { return mtu; }
}
