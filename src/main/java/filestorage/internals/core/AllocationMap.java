package filestorage.internals.core;

import filestorage.utils.Utils;

import java.util.Arrays;
import java.util.List;

public class AllocationMap implements Page {

    private final PageHeader pageHeader;

    private final BitMap bitMap;
    private final int allocationMapIndex;

    public AllocationMap(int allocationMapIndex) {
        this.allocationMapIndex = allocationMapIndex;
        this.bitMap = new BitMap(BitMap.Bitmap_Size_Bt);
        this.pageHeader = new PageHeader();
    }

    private AllocationMap(int allocationMapIndex, PageHeader pageHeader, BitMap bitMap) {
        this.allocationMapIndex = allocationMapIndex;
        this.bitMap = bitMap;
        this.pageHeader = pageHeader;
    }

    public int getAllocationMapIndex() {
        return allocationMapIndex;
    }

    public boolean canAllocateNewPage() {
        return bitMap.getAmountFree() > 0;
    }

    public int getAndAllocateNextAvailablePageId() {
        int res = bitMap.getNextFree();

        if (res < 0) {
            return -1;
        }

        setAllocated(res);

        return res;
    }

    public List<Integer> getAndAllocateNextAvailablePageId(int length) {
        long requiredAmountOfPages = length / FreeSpaceDefault;
        requiredAmountOfPages += length % FreeSpaceDefault == 0 ? 0 : 1;
        List<Integer> nextFreeInRow = bitMap.getNextFreeInRow(requiredAmountOfPages); //todo:tsekot: make it concurent, atomic -> getAndAllocate

        nextFreeInRow.forEach(id -> setAllocated(id));
        return nextFreeInRow;
    }

    public void releasePageId(int pageId) {
        bitMap.unset(pageId);
    }

    public byte[] toBytes() {
        byte[] headerBytes = pageHeader.toBytes();
        byte[] bitMapBytes = bitMap.toBytes();
        return Utils.asOne(headerBytes, bitMapBytes);
    }

    public long filePosition() {
        long l = calculateAllocationMapFilePosition(allocationMapIndex);
        return l;
    }

    private void setAllocated(int pageId) {
        bitMap.set(pageId);
    }

    public static long calculateAllocationMapFilePosition(int allocationMapIndex) {
        long l = allocationMapIndex * ((BitMap.Bitmap_Size_Bt + 1) * Size_Bt);
        return l;
    }

    public static long calculatePageFilePosition(int allocationMapIndex, Integer dataPadeId) {
        long l = calculateAllocationMapFilePosition(allocationMapIndex) + Size_Bt + (dataPadeId * Size_Bt);
        return l;
    }

    public static AllocationMap fromBytes(int allocationMapIndex, byte[] bytes) {
        byte[] bitMapBytes = Arrays.copyOfRange(bytes, PageHeader_Size_Bt, bytes.length);
        return new AllocationMap(allocationMapIndex, new PageHeader(), BitMap.fromBytes(bitMapBytes));
    }

    public boolean canAllocateNewExtent(int length) {
        long requiredAmountOfPages = length / FreeSpaceDefault;
        requiredAmountOfPages += length % FreeSpaceDefault == 0 ? 0 : 1;

        return bitMap.getMaxAmountFreeInRow() >= requiredAmountOfPages;
    }

    public int getAmountOfFreeSlots() {
        return bitMap.getAmountFree();
    }

    static class PageHeader {

        public PageHeader fromBytes(byte[] bytes) {
            return new PageHeader();
        }

        public byte[] toBytes() {
            return new byte[PageHeader_Size_Bt];
        }
    }
}

