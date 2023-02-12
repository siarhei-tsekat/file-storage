package filestorage.internals.core;

import filestorage.utils.Utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

public class MappingPage implements Page {

    private final int allocationMapIndex;
    private final int pageId;
    private final long fileLocation;
    private final BitMap bitMap;
    private final MappingPagePageHeader mappingPagePageHeader;

    public MappingPage(int allocationMapIndex, int pageId, int allocationDataMapIndex, long fileLocation) {
        this.allocationMapIndex = allocationMapIndex;
        this.pageId = pageId;
        this.fileLocation = fileLocation;
        this.bitMap = new BitMap(BitMap.Bitmap_Size_Bt);
        this.mappingPagePageHeader = new MappingPagePageHeader(allocationDataMapIndex);
    }

    private MappingPage(int allocationMapIndex, int pageId, long fileLocation, MappingPagePageHeader mappingPagePageHeader, BitMap bitMap) {
        this.allocationMapIndex = allocationMapIndex;
        this.pageId = pageId;
        this.fileLocation = fileLocation;
        this.bitMap = bitMap;
        this.mappingPagePageHeader = mappingPagePageHeader;
    }

    public void markPage(int pageId) {
        bitMap.set(pageId);
    }

    public byte[] toBytes() {
        byte[] indexBytes = mappingPagePageHeader.toBytes();
        byte[] mapBytes = bitMap.toBytes();
        return Utils.asOne(indexBytes, mapBytes);
    }

    public static MappingPage fromBytes(int allocationMapIndex, int pageId, long fileLocation, byte[] bytes) {
        MappingPagePageHeader mappingPagePageHeader = MappingPagePageHeader.fromBytes(Arrays.copyOfRange(bytes, 0, Page.PageHeader_Size_Bt));
        BitMap bitMap = BitMap.fromBytes(Arrays.copyOfRange(bytes, Page.PageHeader_Size_Bt, bytes.length));
        return new MappingPage(allocationMapIndex, pageId, fileLocation, mappingPagePageHeader, bitMap);
    }

    public long filePosition() {
        return fileLocation;
    }

    public int getAllocationMapIndex() {
        return allocationMapIndex;
    }

    public int getAllocationDataMapIndex() {
        return mappingPagePageHeader.allocationDataMapIndex;
    }

    public int getPageId() {
        return pageId;
    }

    public MappingPageIterator getIterator() {
        return new MappingPageIterator(bitMap.iterator());
    }

    public static class MappingPagePageHeader {
        private final int allocationDataMapIndex;

        public MappingPagePageHeader(int allocationDataMapIndex) {
            this.allocationDataMapIndex = allocationDataMapIndex;
        }

        public static MappingPagePageHeader fromBytes(byte[] bytes) {
            int index = Utils.byteToInt(Arrays.copyOfRange(bytes, 0, 4));
            return new MappingPagePageHeader(index);
        }

        public byte[] toBytes() {
            byte[] res = new byte[PageHeader_Size_Bt];
            byte[] intToByte = Utils.intToByte(allocationDataMapIndex);
            System.arraycopy(intToByte, 0, res, 0, 4);
            return res;
        }
    }

    public static class MappingPageIterator implements Iterator<Integer> {

        private final HashSet<Integer> skipIds = new HashSet<>();
        private final Iterator<BitMap.BitMapPair> bitmapIterator;
        private BitMap.BitMapPair bitMapPair;

        public MappingPageIterator(Iterator<BitMap.BitMapPair> bitmapIterator) {
            this.bitmapIterator = bitmapIterator;
        }

        @Override
        public boolean hasNext() {
            if (bitMapPair == null) {

                while (bitmapIterator.hasNext()) {

                    BitMap.BitMapPair next = bitmapIterator.next();

                    if (next.value == BitMap._allocated && !skipIds.contains(next.ind)) {
                        bitMapPair = next;
                        return true;
                    }
                }
                return false;
            }
            else {
                return true;
            }
        }

        @Override
        public Integer next() {
            if (bitMapPair == null) {
                return -1;
            }
            else {
                int res = bitMapPair.ind;
                bitMapPair = null;
                return res;
            }
        }

        public void skip(int skipPageId) {
            skipIds.add(skipPageId);
        }
    }
}


