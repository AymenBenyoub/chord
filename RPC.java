import java.io.*;
import java.net.*;
import java.math.BigInteger;
import java.util.logging.Logger;

public class RPC {
    private static final Logger logger = LogUtil.getLogger("RPC");

    static NodeInfo findSuccessor(NodeInfo n, BigInteger id) {
        return request(n, "FIND_SUCCESSOR " + id);
    }

    static NodeInfo getPredecessor(NodeInfo n) {
        return request(n, "GET_PREDECESSOR");
    }

    static void notify(NodeInfo n, NodeInfo self) {
        request(n, "NOTIFY " + self.id + " " + self.ip + " " + self.port);
    }

    private static NodeInfo request(NodeInfo n, String msg) {
        try (Socket s = new Socket(n.ip, n.port);
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {

            out.println(msg);
            String res = in.readLine();
            if (res == null || res.equals("NULL"))
                return null;

            String[] p = res.split(" ");
            return new NodeInfo(new BigInteger(p[1]), p[2], Integer.parseInt(p[3]));
        } catch (Exception e) {
            logger.warning("RPC request failed: " + e.getMessage());
            return null;
        }
    }

    public static NodeInfo findFileOwner(NodeInfo n, BigInteger id) {
        return request(n, "FIND_FILE " + id);
    }

    public static boolean storeFile(NodeInfo n, BigInteger id, String filename, byte[] data) {
        try (Socket s = new Socket(n.ip, n.port)) {
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();
            String header = "STORE " + id + " " + filename.replaceAll(" ", "_") + " " + data.length + "\n";
            out.write(header.getBytes());
            out.write(data);
            out.flush();

            String res = readLine(in);
            return res != null && res.equals("OK");
        } catch (Exception e) {
            logger.warning("storeFile failed: " + e.getMessage());
            return false;
        }
    }

    public static DownloadResult downloadFile(NodeInfo n, BigInteger id) {
        try (Socket s = new Socket(n.ip, n.port)) {
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();
            String req = "DOWNLOAD " + id + "\n";
            out.write(req.getBytes());
            out.flush();

            String header = readLine(in);
            if (header == null || header.equals("NOTFOUND"))
                return null;
            String[] p = header.split(" ", 3);
            int len = Integer.parseInt(p[1]);
            String fname = p.length >= 3 ? p[2] : ("file_" + id.toString());

            byte[] data = new byte[len];
            int read = 0;
            while (read < len) {
                int r = in.read(data, read, len - read);
                if (r < 0)
                    break;
                read += r;
            }
            DownloadResult res = new DownloadResult();
            res.data = data;
            res.filename = fname;
            return res;
        } catch (Exception e) {
            logger.warning("downloadFile failed: " + e.getMessage());
            return null;
        }
    }

    private static String readLine(InputStream in) throws Exception {
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

    public static class DownloadResult {
        public byte[] data;
        public String filename;
    }

    public static boolean setSuccessor(NodeInfo target, NodeInfo succ) {
        try (Socket s = new Socket(target.ip, target.port)) {
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();
            String header;
            if (succ == null)
                header = "SET_SUCCESSOR NULL\n";
            else
                header = "SET_SUCCESSOR " + succ.id + " " + succ.ip + " " + succ.port + "\n";
            out.write(header.getBytes());
            out.flush();
            String res = readLine(in);
            return res != null && res.equals("OK");
        } catch (Exception e) {
            logger.warning("setSuccessor failed: " + e.getMessage());
            return false;
        }
    }

    public static boolean setPredecessor(NodeInfo target, NodeInfo pred) {
        try (Socket s = new Socket(target.ip, target.port)) {
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();
            String header;
            if (pred == null)
                header = "SET_PREDECESSOR NULL\n";
            else
                header = "SET_PREDECESSOR " + pred.id + " " + pred.ip + " " + pred.port + "\n";
            out.write(header.getBytes());
            out.flush();
            String res = readLine(in);
            return res != null && res.equals("OK");
        } catch (Exception e) {
            logger.warning("setPredecessor failed: " + e.getMessage());
            return false;
        }
    }
}
