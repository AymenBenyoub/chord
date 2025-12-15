import java.math.BigInteger;
import java.util.logging.Logger;

public class ChordNode {
    private final Logger logger = LogUtil.getLogger("ChordNode-" + Thread.currentThread().threadId());

    public NodeInfo self;
    public volatile NodeInfo successor;
    public volatile NodeInfo predecessor;
    public NodeInfo[] finger;

    public ChordNode(String ip, int port) {
        this.self = new NodeInfo(HashUtil.hash(ip + ":" + port), ip, port);
        this.successor = self;
        this.predecessor = null;
        finger = new NodeInfo[HashUtil.M];
        for (int i = 0; i < HashUtil.M; i++)
            finger[i] = self;

        logger.info("Node initialized: " + self);
    }

    public void join(NodeInfo contact) {
        if (contact == null) {
            successor = self;
            predecessor = null;
            logger.info("Starting new ring");
        } else {
            successor = RPC.findSuccessor(contact, self.id);
            logger.info("Joined ring via contact: " + contact + " ; Successor: " + successor);
        }
    }

    public NodeInfo findSuccessor(BigInteger id) {
        if (HashUtil.inInterval(id, self.id, successor.id)) {
            logger.info("Successor of " + id + " is " + successor);
            return successor;
        }
        NodeInfo n0 = closestPrecedingNode(id);
        if (n0.equals(self))
            return self;

        logger.info("Routing lookup of " + id + " via " + n0);
        return RPC.findSuccessor(n0, id);
    }

    private NodeInfo closestPrecedingNode(BigInteger id) {
        for (int i = HashUtil.M - 1; i >= 0; i--) {
            NodeInfo f = finger[i];
            if (f != null && HashUtil.inInterval(f.id, self.id, id)) {
                return f;
            }
        }
        return self;
    }

    public void stabilize() {
        try {
            NodeInfo x = RPC.getPredecessor(successor);
            if (x != null && HashUtil.inInterval(x.id, self.id, successor.id)) {
                successor = x;
                logger.info("Stabilize: Updated successor to " + successor);
            }
            RPC.notify(successor, self);
        } catch (Exception e) {
            logger.warning("Stabilize failed: " + e.getMessage());
        }
    }

    public void notify(NodeInfo n) {
        if (predecessor == null || HashUtil.inInterval(n.id, predecessor.id, self.id)) {
            predecessor = n;
            logger.info("Notify: Updated predecessor to " + predecessor);
        }
    }

    public void fixFingers() {
        for (int i = 0; i < HashUtil.M; i++) {
            BigInteger start = self.id.add(BigInteger.valueOf(2).pow(i))
                    .mod(HashUtil.TWO_POW_M);
            NodeInfo oldFinger = finger[i];
            finger[i] = findSuccessor(start);
            if (!finger[i].equals(oldFinger)) {
                logger.info("Finger[" + i + "] updated: " + oldFinger + " -> " + finger[i]);
            }
        }
    }

    public void printState() {
        StringBuilder sb = new StringBuilder("\n=== Node State ===\n");
        sb.append("Self: ").append(self).append("\n");
        sb.append("Predecessor: ").append(predecessor).append("\n");
        sb.append("Successor: ").append(successor).append("\n");
        sb.append("Finger Table:\n");
        for (int i = 0; i < finger.length; i++)
            sb.append("[").append(i).append("] -> ").append(finger[i]).append("\n");
        sb.append("=================\n");
        logger.info(sb.toString());
    }
}
