package filestorage.internals.core;

import java.util.PriorityQueue;

public class IndexLeafPage implements Page {
    private final IndexLeafPageHeader pageHeader;
    private final PriorityQueue<Index> indices;

    public IndexLeafPage(int pageId) {
        this.pageHeader = new IndexLeafPageHeader(pageId);
        this.indices = new PriorityQueue<>((a, b) -> Long.compare(a.index, b.index));
    }

    public void addIndex(long index, int allocationMapIndex, int dataPageId, int offset) {
        indices.add(new Index(index, allocationMapIndex, dataPageId, offset));
    }

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }

    @Override
    public long filePosition() {
        return 0;
    }

    public Index getIndex(long documentIndex) {
        for (Index index : indices) {
            if (index.index == documentIndex) {
                return index;
            }
        }
        return null;
    }

    public boolean canAllocateOneMore() {
        return Size_Bt - PageHeader_Size_Bt - (indices.size() * Index.size) >= Index.size;
    }

    public static class Index {
        public static int size = 3 * 4 + 8;
        long index;
        int allocationMapIndex;
        int dataPageId;
        int offset;

        public Index(long index, int allocationMapIndex, int dataPageId, int offset) {
            this.index = index;
            this.allocationMapIndex = allocationMapIndex;
            this.dataPageId = dataPageId;
            this.offset = offset;
        }

        public int getDataPageId() {
            return dataPageId;
        }

        public int getDataAllocationMapIndex() {
            return allocationMapIndex;
        }

        public int getOffset() {
            return offset;
        }
    }

    private static class IndexLeafPageHeader {
        int pageId;

        public IndexLeafPageHeader(int pageId) {
            this.pageId = pageId;
        }
    }
}

