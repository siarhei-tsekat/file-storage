package filestorage.internals.core;

public class UnpackRes {
    public final byte[] res;
    public final int endPos;

    public UnpackRes(byte[] res, int endPos) {
        this.res = res;
        this.endPos = endPos;
    }
}
