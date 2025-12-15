import java.math.BigInteger;

public class NodeInfo {
    public BigInteger id;
    public String ip;
    public int port;

    public NodeInfo(BigInteger id, String ip, int port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    @Override
    public String toString() {
        return id + " " + ip + " " + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        NodeInfo nodeInfo = (NodeInfo) o;
        return port == nodeInfo.port && id.equals(nodeInfo.id) && ip.equals(nodeInfo.ip);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + ip.hashCode();
        result = 31 * result + Integer.hashCode(port);
        return result;
    }
}
