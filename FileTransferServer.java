import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileTransferServer extends Thread {
    private final int port;
    private final ChordNode node;

    public FileTransferServer(ChordNode node, int port) {
        this.node = node;
        this.port = port;
    }

    public void run() {
        try (ServerSocket server = new ServerSocket(port)) {
            while (true) {
                Socket s = server.accept();
                new Thread(() -> handle(s)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handle(Socket s) {
        try (InputStream in = s.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {

            String header = reader.readLine();
            if (header == null)
                return;
            String[] parts = header.split(" ", 3);
            if (parts.length < 3)
                return;

            if (parts[0].equals("FILE_OFFER")) {
                String fileName = parts[1];
                long totalSize = Long.parseLong(parts[2]);

                File dir = new File("received");
                if (!dir.exists())
                    dir.mkdirs();
                File outFile = new File(dir, fileName);
                long existing = outFile.exists() ? outFile.length() : 0L;

                out.println("OK " + existing);

                try (RandomAccessFile raf = new RandomAccessFile(outFile, "rw")) {
                    raf.seek(existing);
                    byte[] buffer = new byte[8192];
                    long remaining = totalSize - existing;
                    InputStream raw = s.getInputStream();
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buffer.length, remaining);
                        int r = raw.read(buffer, 0, toRead);
                        if (r == -1)
                            break;
                        raf.write(buffer, 0, r);
                        remaining -= r;
                    }
                }

                // rename with timestamp to avoid overwriting
                String ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
                File saved = new File(dir, ts + "_" + fileName);
                outFile.renameTo(saved);

                node.notifyFileReceived(saved);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                s.close();
            } catch (IOException ignored) {
            }
        }
    }
}
