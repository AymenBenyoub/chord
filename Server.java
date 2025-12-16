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
        try (BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {

            String[] cmd = in.readLine().split(" ");
            logger.info("Received RPC: " + String.join(" ", cmd));

            switch (cmd[0]) {
                case "FIND_SUCCESSOR":
                    out.println("NODE " + node.findSuccessor(new BigInteger(cmd[1])));
                    break;
                case "GET_PREDECESSOR":
                    out.println(node.predecessor == null ? "NULL" : "NODE " + node.predecessor);
                    break;
                case "NOTIFY":
                    if (cmd.length >= 5) {
                        node.notify(new NodeInfo(new BigInteger(cmd[1]), cmd[2], Integer.parseInt(cmd[3]),
                                Integer.parseInt(cmd[4])));
                    } else {
                        node.notify(new NodeInfo(new BigInteger(cmd[1]), cmd[2], Integer.parseInt(cmd[3])));
                    }
                    out.println("OK");
                    break;
                default:
                    out.println("NULL");
            }
        } catch (Exception e) {
            logger.warning("RPC handling failed: " + e.getMessage());
        }
    }

}
