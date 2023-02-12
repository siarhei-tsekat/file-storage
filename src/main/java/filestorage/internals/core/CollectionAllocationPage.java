package filestorage.internals.core;

import filestorage.domain.CollectionName;
import filestorage.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollectionAllocationPage implements Page {

    public static final int _1KBT_SIZE = 1024;

    private final int id;
    private final CollectionName collectionName;
    private final List<MappingPageInfo> mappingPageInfoList = new ArrayList<>();

    public CollectionAllocationPage(int pageId, CollectionName collectionName, MappingPage mappingPage) {
        this.id = pageId;
        this.collectionName = collectionName;
        this.mappingPageInfoList.add(new MappingPageInfo(mappingPage.getAllocationMapIndex(), mappingPage.getPageId()));
    }

    private CollectionAllocationPage(int pageId, CollectionName collectionName, List<MappingPageInfo> mappingPageInfoList) {
        this.id = pageId;
        this.collectionName = collectionName;
        this.mappingPageInfoList.addAll(mappingPageInfoList);
    }

    public List<MappingPageInfo> getMappingPageInfoList() {
        return mappingPageInfoList;
    }

    public CollectionName getCollectionName() {
        return collectionName;
    }

    public long filePosition() {
        return id * _1KBT_SIZE;
    }

    public byte[] toBytes() {
        byte[] res = new byte[_1KBT_SIZE];
        int pos = 0;

        byte[] collectionNameBytes = Utils.pack(collectionName.toString());
        System.arraycopy(collectionNameBytes, 0, res, pos, collectionNameBytes.length);
        pos += collectionNameBytes.length;

        byte[] amountBytes = Utils.intToByte(mappingPageInfoList.size());
        System.arraycopy(amountBytes, 0, res, pos, amountBytes.length);
        pos += amountBytes.length;

        for (MappingPageInfo mappingPageInfo : mappingPageInfoList) {
            System.arraycopy(mappingPageInfo.toBytes(), 0, res, pos, MappingPageInfo.SIZE_BYTES);
            pos += MappingPageInfo.SIZE_BYTES;
        }

        return res;
    }

    public static CollectionAllocationPage fromBytes(int index, byte[] bytes) {
        UnpackRes collectionNameUnpackRes = Utils.unpack(0, bytes);
        int amount = Utils.byteToInt(Arrays.copyOfRange(bytes, collectionNameUnpackRes.endPos, collectionNameUnpackRes.endPos + 4));

        int pos = collectionNameUnpackRes.endPos + 4;

        List<MappingPageInfo> pages = new ArrayList<>();

        for (int i = 0; i < amount; i++) {
            pages.add(MappingPageInfo.fromBytes(Arrays.copyOfRange(bytes, pos, pos + MappingPageInfo.SIZE_BYTES)));
        }
        return new CollectionAllocationPage(index, new CollectionName(new String(collectionNameUnpackRes.res)), pages);
    }
}

