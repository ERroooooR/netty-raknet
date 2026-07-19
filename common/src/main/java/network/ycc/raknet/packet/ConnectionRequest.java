package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

public class ConnectionRequest extends SimpleFramedPacket implements Packet.ClientIdConnection {

    protected long clientId;
    protected long timestamp;
    protected long transportFeatures;

    public ConnectionRequest() {
        reliability = Reliability.RELIABLE;
    }

    public ConnectionRequest(long clientId) {
        this();
        this.clientId = clientId;
        this.timestamp = System.nanoTime();
    }

    public ConnectionRequest(long clientId, long transportFeatures) {
        this(clientId);
        this.transportFeatures = transportFeatures;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(clientId);
        buf.writeLong(timestamp);
        buf.writeBoolean(false);
        if (transportFeatures != 0) {
            buf.writeInt(network.ycc.raknet.TransportFeatures.MAGIC);
            buf.writeLong(transportFeatures);
        }
    }

    @Override
    public void decode(ByteBuf buf) {
        clientId = buf.readLong(); //client id
        timestamp = buf.readLong();
        buf.readBoolean(); //use security
        if (buf.readableBytes() >= 12
                && buf.getInt(buf.readerIndex()) == network.ycc.raknet.TransportFeatures.MAGIC) {
            buf.skipBytes(4);
            transportFeatures = buf.readLong();
        }
    }

    public long getClientId() {
        return clientId;
    }

    public void setClientId(long clientId) {
        this.clientId = clientId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTransportFeatures() { return transportFeatures; }

}
