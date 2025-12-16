import java.io.*;
import java.math.BigInteger;
import java.nio.file.*;
import java.util.logging.Logger;

public class FileStore {
    private final Path dir;
    private final Logger logger = LogUtil.getLogger("FileStore");

    public FileStore(int port) {
        this.dir = Paths.get("data_" + port);
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("File store at: " + dir.toAbsolutePath());
    }

    public synchronized void store(BigInteger id, String filename, byte[] data) throws IOException {
        Path meta = dir.resolve(id.toString() + ".meta");
        Path file = dir.resolve(id.toString() + ".bin");
        Files.write(file, data);
        Files.write(meta, filename.getBytes());
        logger.info("Stored file id=" + id + " name=" + filename + " size=" + data.length);
    }

    public synchronized boolean has(BigInteger id) {
        return Files.exists(dir.resolve(id.toString() + ".bin"));
    }

    public synchronized File get(BigInteger id) {
        Path p = dir.resolve(id.toString() + ".bin");
        return Files.exists(p) ? p.toFile() : null;
    }

    public synchronized String getFilename(BigInteger id) throws IOException {
        Path meta = dir.resolve(id.toString() + ".meta");
        if (!Files.exists(meta))
            return null;
        return new String(Files.readAllBytes(meta));
    }

    public synchronized String[] listFiles() {
        try {
            return Files.list(dir)
                    .filter(p -> p.getFileName().toString().endsWith(".meta"))
                    .map(p -> p.getFileName().toString().replaceFirst("\\.meta$", ""))
                    .toArray(String[]::new);
        } catch (IOException e) {
            return new String[0];
        }
    }

    public synchronized void remove(BigInteger id) {
        try {
            Files.deleteIfExists(dir.resolve(id.toString() + ".meta"));
            Files.deleteIfExists(dir.resolve(id.toString() + ".bin"));
            logger.info("Removed file id=" + id);
        } catch (IOException e) {
            logger.warning("Failed to remove file id=" + id + " : " + e.getMessage());
        }
    }
}
