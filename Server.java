import java.io.*;
import java.net.*;
import java.util.logging.Logger;
import java.math.BigInteger;

public class Server extends Thread {
    private final ChordNode node;
    private final Logger logger = LogUtil.getLogger("Server-" + Thread.currentThread().threadId());

    public Server(ChordNode node) {
        this.node = node;
    }

    public void run() {
        try (ServerSocket server = new ServerSocket(node.self.port)) {
            while (true) {
                Socket s = server.accept();
                new Thread(() -> handle(s)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handle(Socket s) {
        try (InputStream inStream = s.getInputStream();
                OutputStream outStream = s.getOutputStream();
                PrintWriter out = new PrintWriter(outStream, true)) {

            String line = readLine(inStream);
            if (line == null)
                return;
            String[] cmd = line.split(" ");
            logger.info("Received RPC: " + String.join(" ", cmd));

            switch (cmd[0]) {
                case "FIND_SUCCESSOR":
                    out.println("NODE " + node.findSuccessor(new BigInteger(cmd[1])));
                    break;
                case "FIND_FILE": {
                    BigInteger id = new BigInteger(cmd[1]);
                    NodeInfo owner = node.findSuccessor(id);
                    out.println("NODE " + owner);
                    break;
                }
                case "STORE": {
                    // STORE <id> <filename> <length>\n<raw-bytes>
                    BigInteger id = new BigInteger(cmd[1]);
                    String filename = cmd[2];
                    int len = Integer.parseInt(cmd[3]);
                    byte[] data = new byte[len];
                    int read = 0;
                    while (read < len) {
                        int r = inStream.read(data, read, len - read);
                        if (r < 0)
                            break;
                        read += r;
                    }
                    try {
                        node.fileStore.store(id, filename, data);
                        out.println("OK");
                    } catch (Exception ex) {
                        out.println("ERROR");
                    }
                    break;
                }
                case "DOWNLOAD": {
                    BigInteger id = new BigInteger(cmd[1]);
                    if (!node.fileStore.has(id)) {
                        out.println("NOTFOUND");
                    } else {
                        File f = node.fileStore.get(id);
                        String fname = node.fileStore.getFilename(id);
                        long len = f.length();
                        out.println("OK " + len + " " + (fname == null ? f.getName() : fname));
                        try (FileInputStream fis = new FileInputStream(f)) {
                            byte[] buf = new byte[8192];
                            int r;
                            while ((r = fis.read(buf)) != -1) {
                                outStream.write(buf, 0, r);
                            }
                            outStream.flush();
                        }
                    }
                    break;
                }
                case "GET_PREDECESSOR":
                    out.println(node.predecessor == null ? "NULL" : "NODE " + node.predecessor);
                    break;
                case "SET_SUCCESSOR": {
                    if (cmd[1].equals("NULL")) {
                        node.successor = node.self;
                    } else {
                        node.successor = new NodeInfo(new BigInteger(cmd[1]), cmd[2], Integer.parseInt(cmd[3]));
                    }
                    out.println("OK");
                    break;
                }
                case "SET_PREDECESSOR": {
                    if (cmd[1].equals("NULL")) {
                        node.predecessor = null;
                    } else {
                        node.predecessor = new NodeInfo(new BigInteger(cmd[1]), cmd[2], Integer.parseInt(cmd[3]));
                    }
                    out.println("OK");
                    break;
                }
                case "NOTIFY":
                    node.notify(new NodeInfo(new BigInteger(cmd[1]), cmd[2], Integer.parseInt(cmd[3])));
                    out.println("OK");
                    break;
                default:
                    out.println("NULL");
            }
        } catch (Exception e) {
            logger.warning("RPC handling failed: " + e.getMessage());
        }
    }

    private String readLine(InputStream in) throws Exception {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n')
                break;
            if (b == '\r')
                continue;
            sb.append((char) b);
        }
        if (sb.length() == 0 && b == -1)
            return null;
        return sb.toString();
    }
}
