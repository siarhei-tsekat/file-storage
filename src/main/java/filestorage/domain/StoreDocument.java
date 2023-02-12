package filestorage.domain;

import filestorage.internals.core.UnpackRes;
import filestorage.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StoreDocument {

    enum State {
        Transient,
        Detached
    }

    private final State state;
    private Long id;
    private final HashMap<String, String> map = new HashMap<>();

    public StoreDocument(Long id) {
        this.id = id;
        map.put("_id", id.toString());
        state = State.Transient;
    }

    public StoreDocument(UUID uuid) {
        this.id = uuid.getMostSignificantBits() & Long.MAX_VALUE;
        map.put("_id", id.toString());
        state = State.Transient;
    }

    private StoreDocument() {
        state = State.Detached;
    }

    public void put(String fieldName, String fieldValue) {
        map.put(fieldName, fieldValue);
    }

    public String get(String key) {
        return map.get(key);
    }

    public Long getId() {
        return id;
    }

    byte[] toBytes() {
        List<byte[]> pair = new ArrayList<>();
        int totalLength = 0;

        for (Map.Entry<String, String> entry : map.entrySet()) {

            String fieldName = entry.getKey();
            byte[] fieldNameBytes = Utils.pack(fieldName);

            String fieldValue = entry.getValue();
            byte[] fieldValueBytes = Utils.pack(fieldValue);

            pair.add(fieldNameBytes);
            pair.add(fieldValueBytes);
            totalLength += fieldNameBytes.length + fieldValueBytes.length;
        }

        byte[] res = new byte[totalLength];
        int position = 0;

        for (byte[] bytes : pair) {
            System.arraycopy(bytes, 0, res, position, bytes.length);
            position += bytes.length;
        }
        return res;
    }

    public String toString() {
        return map.toString();
    }

    private void setId(Long id) {
        this.id = id;
    }

    static StoreDocument fromBytes(byte[] data) {

        StoreDocument storeDocument = new StoreDocument();

        int ind = 0;

        while (ind < data.length) {

            UnpackRes fieldNameUnpackRes = Utils.unpack(ind, data);
            UnpackRes fieldValueUnpackRes = Utils.unpack(fieldNameUnpackRes.endPos, data);

            String fieldName = new String(fieldNameUnpackRes.res);
            String fieldValue = new String(fieldValueUnpackRes.res);

            storeDocument.put(fieldName, fieldValue);
            ind = fieldValueUnpackRes.endPos;
        }
        storeDocument.setId(Long.parseLong(storeDocument.get("_id")));
        return storeDocument;
    }
}
