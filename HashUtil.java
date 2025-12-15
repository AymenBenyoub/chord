import java.math.BigInteger;
import java.security.MessageDigest;

public class HashUtil {
    public static final int M = 5;
    public static final BigInteger TWO_POW_M = BigInteger.valueOf(2).pow(M);

    public static BigInteger hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            return new BigInteger(1, md.digest(input.getBytes())).mod(TWO_POW_M);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean inInterval(BigInteger id, BigInteger start, BigInteger end) {
        if (start.compareTo(end) < 0) {
            return id.compareTo(start) > 0 && id.compareTo(end) <= 0; // (start, end]
        } else { // wrap around
            return id.compareTo(start) > 0 || id.compareTo(end) <= 0;
        }
    }

}
