import java.math.BigInteger;

public class NodeInfo {
    public BigInteger id;
    public String ip;
    public int port;
    public int filePort;

    public NodeInfo(BigInteger id, String ip, int port) {
        this(id, ip, port, port + 1000);
    }

    public NodeInfo(BigInteger id, String ip, int port, int filePort) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.filePort = filePort;
    }

    @Override
    public String toString() {
        return id + " " + ip + " " + port + " " + filePort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        NodeInfo nodeInfo = (NodeInfo) o;
        return port == nodeInfo.port && filePort == nodeInfo.filePort && id.equals(nodeInfo.id)
                && ip.equals(nodeInfo.ip);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + ip.hashCode();
        result = 31 * result + Integer.hashCode(port);
        result = 31 * result + Integer.hashCode(filePort);
        return result;
    }
}
