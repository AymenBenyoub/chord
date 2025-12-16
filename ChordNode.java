import java.math.BigInteger;
import java.util.logging.Logger;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;

public class ChordNode {
    private final Logger logger = LogUtil.getLogger("ChordNode-" + Thread.currentThread().threadId());

    public NodeInfo self;
    public volatile NodeInfo successor;
    public volatile NodeInfo predecessor;
    public NodeInfo[] finger;
    public FileStore fileStore;

    public ChordNode(String ip, int port) {
        this.self = new NodeInfo(HashUtil.hash(ip + ":" + port), ip, port);
        this.fileStore = new FileStore(port);
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
            // notify successor asynchronously so it can update predecessor and transfer
            // keys
            new Thread(() -> RPC.notify(successor, self)).start();
            // also give successor time to process and transfer keys
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // ignore
            }
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
        NodeInfo oldPred = predecessor;
        if (predecessor == null || HashUtil.inInterval(n.id, predecessor.id, self.id)) {
            predecessor = n;
            logger.info("Notify: Updated predecessor to " + predecessor);
            // predecessor changed: redistribute any keys that are no longer ours
            new Thread(() -> redistributeKeys()).start();
        }
    }

    private void redistributeKeys() {
        try {
            String[] ids = fileStore.listFiles();
            if (ids == null || ids.length == 0)
                return;
            for (String idStr : ids) {
                try {
                    BigInteger id = new BigInteger(idStr);
                    NodeInfo owner = findSuccessor(id);
                    if (owner == null)
                        continue;
                    if (!owner.equals(self)) {
                        File f = fileStore.get(id);
                        if (f == null)
                            continue;
                        String fname = fileStore.getFilename(id);
                        byte[] data = Files.readAllBytes(f.toPath());
                        boolean ok = RPC.storeFile(owner, id, fname == null ? f.getName() : fname, data);
                        if (ok) {
                            fileStore.remove(id);
                            logger.info("Transferred file id=" + id + " to " + owner);
                        } else {
                            logger.warning("Failed to transfer file id=" + id + " to " + owner);
                        }
                    }
                } catch (Exception ex) {
                    logger.warning("Redistribute error for id=" + idStr + " : " + ex.getMessage());
                }
            }
        } catch (Exception e) {
            logger.warning("RedistributeKeys failed: " + e.getMessage());
        }
    }

    public void leave() {
        try {
            if (successor == null || successor.equals(self)) {
                logger.info("Leaving: no successor to transfer keys to (single node).");
                return;
            }
            String[] ids = fileStore.listFiles();
            if (ids == null || ids.length == 0)
                return;
            for (String idStr : ids) {
                try {
                    BigInteger id = new BigInteger(idStr);
                    File f = fileStore.get(id);
                    if (f == null)
                        continue;
                    String fname = fileStore.getFilename(id);
                    byte[] data = Files.readAllBytes(f.toPath());
                    boolean ok = RPC.storeFile(successor, id, fname == null ? f.getName() : fname, data);
                    if (ok) {
                        fileStore.remove(id);
                        logger.info("Left: transferred file id=" + id + " to " + successor);
                    } else {
                        logger.warning("Leave: failed to transfer file id=" + id + " to " + successor);
                    }
                } catch (Exception ex) {
                    logger.warning("Leave transfer error for id=" + idStr + " : " + ex.getMessage());
                }
            }
            // update neighbors so topology no longer includes this node
            try {
                if (predecessor != null && successor != null) {
                    RPC.setSuccessor(predecessor, successor);
                    RPC.setPredecessor(successor, predecessor);
                } else if (predecessor == null && successor != null) {
                    RPC.setPredecessor(successor, null);
                } else if (successor == null && predecessor != null) {
                    RPC.setSuccessor(predecessor, null);
                }
            } catch (Exception ex) {
                logger.warning("Neighbor update during leave failed: " + ex.getMessage());
            }

            // mark self as standalone
            successor = self;
            predecessor = null;

        } catch (Exception e) {
            logger.warning("Leave failed: " + e.getMessage());
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
