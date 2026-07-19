package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;

public class ServerHandshake extends SimpleFramedPacket {

    private InetSocketAddress clientAddr;
    private long timestamp;
    private int nExtraAddresses;
    private long transportFeatures;

    public ServerHandshake() {
        reliability = Reliability.RELIABLE;
    }

    public ServerHandshake(InetSocketAddress clientAddr, long timestamp) {
        this(clientAddr, timestamp, 20);
    }

    public ServerHandshake(InetSocketAddress clientAddr, long timestamp, int nExtraAddresses) {
        this();
        this.clientAddr = clientAddr;
        this.timestamp = timestamp;
        this.nExtraAddresses = nExtraAddresses;
    }

    public ServerHandshake(InetSocketAddress clientAddr, long timestamp, int nExtraAddresses, long transportFeatures) {
        this(clientAddr, timestamp, nExtraAddresses);
        this.transportFeatures = transportFeatures;
    }

    @Override
    public void encode(ByteBuf buf) {
        writeAddress(buf, clientAddr);
        buf.writeShort(0);
        for (int i = 0; i < nExtraAddresses; i++) {
            writeAddress(buf);
        }
        if (transportFeatures != 0) {
            buf.writeInt(network.ycc.raknet.TransportFeatures.MAGIC);
            buf.writeLong(transportFeatures);
        }
        buf.writeLong(timestamp);
        buf.writeLong(System.currentTimeMillis());
    }

    @Override
    public void decode(ByteBuf buf) {
        clientAddr = readAddress(buf);
        buf.readShort();
        for (nExtraAddresses = 0; buf.readableBytes() > 16; nExtraAddresses++) {
            if (buf.readableBytes() == 28
                    && buf.getInt(buf.readerIndex()) == network.ycc.raknet.TransportFeatures.MAGIC) {
                buf.skipBytes(4);
                transportFeatures = buf.readLong();
                break;
            }
            readAddress(buf);
        }
        timestamp = buf.readLong();
        buf.readLong(); // server timestamp; the echoed client timestamp above is used by ClientHandshake
    }

    public InetSocketAddress getClientAddr() {
        return clientAddr;
    }

    public void setClientAddr(InetSocketAddress clientAddr) {
        this.clientAddr = clientAddr;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getnExtraAddresses() {
        return nExtraAddresses;
    }

    public void setnExtraAddresses(int nExtraAddresses) {
        this.nExtraAddresses = nExtraAddresses;
    }

    public long getTransportFeatures() { return transportFeatures; }

}
