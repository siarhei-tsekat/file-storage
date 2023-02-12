package filestorage.internals.core;

public class DataRow {

    private byte[] data;

    DataRow(byte[] data) {
        this.data = data;
    }

    public byte[] toBytes() {
        return data;
    }

    public byte[] getData() {
        return data;
    }
}
