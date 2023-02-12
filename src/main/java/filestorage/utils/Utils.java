package filestorage.utils;

import filestorage.internals.core.UnpackRes;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class Utils {

    public static byte[] intToByte(int v) {

        byte[] res = new byte[4];

        for (int i = 3; i >= 0; i--) {
            res[3 - i] = (byte) (v >> (i * 8) & 255);
        }

        return res;
    }

    public static int byteToInt(byte[] arr) {

        int res = 0;

        for (int i = 0; i < 4; i++) {
            res = (res << 8) + (arr[i] & 255);
        }

        return res;
    }

    public static byte[] longToByte(Long v) {
        byte[] res = new byte[8];

        for (int i = 7; i >= 0; i--) {
            res[7 - i] = (byte) (v >> (i * 8) & 255);
        }

        return res;
    }

    public static long byteToLong(byte[] arr) {
        long res = 0;

        for (int i = 0; i < 8; i++) {
            res = (res << 8) + (arr[i] & 255);
        }

        return res;
    }

    public static byte[] pack(String string) {
        return pack(string.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] pack(byte[] bytes) {

        int length = bytes.length;
        byte[] lengthBytes = intToByte(length);
        byte[] res = new byte[4 + length];

        System.arraycopy(lengthBytes, 0, res, 0, 4);
        System.arraycopy(bytes, 0, res, 4, length);

        return res;
    }

    public static byte[] asOne(byte[]... bytes) {
        int totalLength = 0;

        for (byte[] aByte : bytes) {
            totalLength += aByte.length;
        }

        byte[] res = new byte[totalLength];
        int ind = 0;

        for (byte[] aByte : bytes) {
            System.arraycopy(aByte, 0, res, ind, aByte.length);
            ind += aByte.length;
        }
        return res;
    }

    public static byte[] asOne(int sizeOf, byte[]... bytes) {

        byte[] res = new byte[sizeOf];
        int ind = 0;

        for (byte[] aByte : bytes) {
            System.arraycopy(aByte, 0, res, ind, aByte.length);
            ind += aByte.length;
        }
        return res;
    }

    public static byte[] asOne(List<byte[]> bytes) {
        int sizeOf = 0;

        for (byte[] aByte : bytes) {
            sizeOf+=aByte.length;
        }

        byte[] res = new byte[sizeOf];
        int ind = 0;

        for (byte[] aByte : bytes) {
            System.arraycopy(aByte, 0, res, ind, aByte.length);
            ind += aByte.length;
        }
        return res;
    }

    public static UnpackRes unpack(int from, byte[] bytes) {

        int length = byteToInt(Arrays.copyOfRange(bytes, from, from + 4));
        int endPos = from + 4 + length;
        byte[] res = Arrays.copyOfRange(bytes, from + 4, endPos);

        return new UnpackRes(res, endPos);
    }

    public static long unpackLong(int ind, byte[] bytes) {
        return byteToLong(Arrays.copyOfRange(bytes, ind, ind + 8));
    }
}
