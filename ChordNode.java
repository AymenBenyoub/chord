import java.math.BigInteger;
import java.util.logging.Logger;

public class ChordNode {
    private final Logger logger = LogUtil.getLogger("ChordNode-" + Thread.currentThread().threadId());

    public NodeInfo self;
    public volatile NodeInfo successor;
    public volatile NodeInfo predecessor;
    public NodeInfo[] finger;
    public static final int FILE_PORT_OFFSET = 1000;
    private FileTransferServer ftServer;

    public ChordNode(String ip, int port) {
        this(ip, port, port + FILE_PORT_OFFSET);
    }

    public ChordNode(String ip, int port, int filePort) {
        this.self = new NodeInfo(HashUtil.hash(ip + ":" + port), ip, port, filePort);
        this.successor = self;
        this.predecessor = null;
        finger = new NodeInfo[HashUtil.M];
        for (int i = 0; i < HashUtil.M; i++)
            finger[i] = self;

        logger.info("Node initialized: " + self);

        // start file transfer server on provided file port
        ftServer = new FileTransferServer(this, filePort);
        ftServer.start();
    }

    public boolean sendFile(NodeInfo dest, java.io.File file) {
        return sendFile(dest, file, null);
    }

    public boolean sendFile(NodeInfo dest, java.io.File file,
            java.util.function.BiConsumer<Long, Long> progressCallback) {
        if (dest == null || file == null || !file.exists())
            return false;
        int port = dest.filePort > 0 ? dest.filePort : dest.port + FILE_PORT_OFFSET;
        final int MAX_RETRIES = 3;
        final int CONNECT_TIMEOUT = 5000;

        long total = file.length();
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try (java.net.Socket s = new java.net.Socket()) {
                s.connect(new java.net.InetSocketAddress(dest.ip, port), CONNECT_TIMEOUT);
                s.setSoTimeout(10000);
                try (java.io.OutputStream out = s.getOutputStream();
                        java.io.PrintWriter writer = new java.io.PrintWriter(out, true);
                        java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "r")) {

                    // Offer file and ask for existing offset
                    writer.println("FILE_OFFER " + file.getName() + " " + total);
                    java.io.BufferedReader in = new java.io.BufferedReader(
                            new java.io.InputStreamReader(s.getInputStream()));
                    String resp = in.readLine();
                    long offset = 0;
                    if (resp != null && resp.startsWith("OK")) {
                        String[] toks = resp.split(" ");
                        if (toks.length >= 2)
                            offset = Long.parseLong(toks[1]);
                    }

                    if (offset > 0)
                        raf.seek(offset);
                    byte[] buffer = new byte[8192];
                    long sent = offset;
                    int r;
                    while ((r = raf.read(buffer)) != -1) {
                        out.write(buffer, 0, r);
                        sent += r;
                        if (progressCallback != null)
                            progressCallback.accept(sent, total);
                    }
                    out.flush();
                    logger.info("Sent file " + file.getName() + " to " + dest + " (" + total + " bytes)");
                    return true;
                }
            } catch (Exception e) {
                logger.warning("File send attempt " + attempt + " failed: " + e.getMessage());
                try {
                    Thread.sleep(1000 * attempt);
                } catch (InterruptedException ignored) {
                }
            }
        }
        return false;
    }

    public void notifyFileReceived(java.io.File f) {
        logger.info("Received file: " + f.getAbsolutePath());
        try {
            ChordUI.notifyFileReceived(f);
        } catch (Throwable ignored) {
        }
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
