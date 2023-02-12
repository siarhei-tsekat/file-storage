package filestorage.internals.core;

import filestorage.utils.Utils;

import java.util.Arrays;

public class MappingPageInfo {

    public static final int SIZE_BYTES = 4 + 4; // allocationMapIndex (4bytes) +  pageId (4bytes)
    private final int allocationMapIndex;
    private final int pageId;

    public MappingPageInfo(int allocationMapIndex, int pageId) {
        this.allocationMapIndex = allocationMapIndex;
        this.pageId = pageId;
    }

    public int getAllocationMapIndex() {
        return allocationMapIndex;
    }

    public int getMappingPageId() {
        return pageId;
    }

    public static MappingPageInfo fromBytes(byte[] bytes) {
        int allocationMapIndex = Utils.byteToInt(Arrays.copyOfRange(bytes, 0, 4));
        int pageId = Utils.byteToInt(Arrays.copyOfRange(bytes, 4, 8));
        return new MappingPageInfo(allocationMapIndex, pageId);
    }

    public byte[] toBytes() {
        byte[] amiBytes = Utils.intToByte(allocationMapIndex);
        byte[] piBytes = Utils.intToByte(pageId);

        return Utils.asOne(amiBytes, piBytes);
    }
}