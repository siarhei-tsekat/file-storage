package filestorage.internals;

import filestorage.internals.core.CollectionAllocationPage;
import filestorage.utils.StoreIO;
import filestorage.utils.Utils;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class StoreMetaInfo {

    private final List<CollectionAllocationPage> allocationPages = new ArrayList<>();

    public StoreMetaInfo(Path storeMetaInfoFilePath) {
        byte[] amountBytes = StoreIO.readFromFile(storeMetaInfoFilePath, 0, 4);
        int count = Utils.byteToInt(amountBytes);
        long pos = 4;

        for (int i = 0; i < count; i++) {
            byte[] collectionAllocationPageBytes = StoreIO.readFromFile(storeMetaInfoFilePath, pos, CollectionAllocationPage._1KBT_SIZE);
            CollectionAllocationPage page = CollectionAllocationPage.fromBytes(i, collectionAllocationPageBytes);
            allocationPages.add(page);
            pos += CollectionAllocationPage._1KBT_SIZE;
        }
    }

    public List<CollectionAllocationPage> getAllCollectionAllocationInfo() {
        return allocationPages;
    }

    public void addCollectionAllocationPage(CollectionAllocationPage collectionAllocationPage) {
        allocationPages.add(collectionAllocationPage);
    }

    public List<CollectionAllocationPage> getAllocationPages() {
        return allocationPages;
    }

    public byte[] toBytes() {
        byte[] res = new byte[4 + allocationPages.size() * CollectionAllocationPage._1KBT_SIZE];
        int pos = 0;

        byte[] amountBytes = Utils.intToByte(allocationPages.size());
        System.arraycopy(amountBytes, 0, res, 0, 4);
        pos += 4;

        for (CollectionAllocationPage allocationInfo : allocationPages) {
            System.arraycopy(allocationInfo.toBytes(), 0, res, pos, CollectionAllocationPage._1KBT_SIZE);
            pos += CollectionAllocationPage._1KBT_SIZE;
        }

        return res;
    }
}
