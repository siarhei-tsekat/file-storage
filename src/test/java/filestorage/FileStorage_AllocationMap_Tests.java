package filestorage;

import filestorage.internals.core.AllocationMap;
import filestorage.internals.core.BitMap;
import filestorage.internals.core.Page;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class FileStorage_AllocationMap_Tests {

    @ParameterizedTest
    @CsvSource({"0,0", "0,100", "0,1000", "1,0", "1,100", "1,1000", "15,0", "15,1", "15,15"})
    public void cover_calculatePageFilePosition(int allocationMap, int pageId) {

        long expected = (allocationMap * (BitMap.Bitmap_Size_Bt + 1) * Page.Size_Bt) + Page.Size_Bt +  pageId * (Page.Size_Bt);
        long actual = AllocationMap.calculatePageFilePosition(allocationMap, pageId);

        Assertions.assertEquals(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 100, 1_000})
    public void cover_calculateAllocationMapFilePosition(int id) {

        long expected = id * ((BitMap.Bitmap_Size_Bt + 1) * Page.Size_Bt);
        long actual = AllocationMap.calculateAllocationMapFilePosition(id);

        Assertions.assertEquals(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 100, 1_000})
    public void cover_getAndAllocateNextAvailablePageId(int id) {
        AllocationMap allocationMap = new AllocationMap(id);

        for (int i = 0; i < BitMap.Bitmap_Size_Bt; i++) {
            Assertions.assertEquals(i, allocationMap.getAndAllocateNextAvailablePageId());
        }
        Assertions.assertEquals(-1, allocationMap.getAndAllocateNextAvailablePageId());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 100, 1_000})
    public void cover_getAndAllocateNextAvailablePageId_releasePageId(int id) {
        AllocationMap allocationMap = new AllocationMap(id);

        Assertions.assertEquals(AllocationMap.calculateAllocationMapFilePosition(id), allocationMap.filePosition());
        Assertions.assertEquals(0, allocationMap.getAndAllocateNextAvailablePageId());
        Assertions.assertEquals(1, allocationMap.getAndAllocateNextAvailablePageId());

        allocationMap.releasePageId(0);

        Assertions.assertEquals(0, allocationMap.getAndAllocateNextAvailablePageId());
        Assertions.assertEquals(2, allocationMap.getAndAllocateNextAvailablePageId());

        allocationMap.releasePageId(1);
        Assertions.assertEquals(1, allocationMap.getAndAllocateNextAvailablePageId());
    }

    @Test
    public void cover_toBytes_fromBytes() {
        AllocationMap allocationMap = new AllocationMap(0);

        Assertions.assertEquals(0, allocationMap.getAndAllocateNextAvailablePageId());
        Assertions.assertEquals(1, allocationMap.getAndAllocateNextAvailablePageId());
        Assertions.assertEquals(2, allocationMap.getAndAllocateNextAvailablePageId());

        byte[] bytes = allocationMap.toBytes();

        AllocationMap deserialized = AllocationMap.fromBytes(0, bytes);

        Assertions.assertEquals(3, deserialized.getAndAllocateNextAvailablePageId());
        deserialized.releasePageId(0);
        Assertions.assertEquals(0, deserialized.getAndAllocateNextAvailablePageId());
        Assertions.assertEquals(4, deserialized.getAndAllocateNextAvailablePageId());

    }
}
