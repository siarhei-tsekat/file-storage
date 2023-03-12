package filestorage.internals;

import filestorage.internals.core.BTree;
import filestorage.internals.core.IndexLeafPage;

import java.nio.file.Path;

public class IndexStore {

    static class IndexKey implements Comparable<IndexKey> {
        int pageId;
        long minDocumentIndex;

        public IndexKey(int pageId, long minDocumentIndex) {
            this.pageId = pageId;
            this.minDocumentIndex = minDocumentIndex;
        }

        @Override
        public int compareTo(IndexKey that) {
            return Long.compare(this.minDocumentIndex, that.minDocumentIndex);
        }
    }

    private final BTree<IndexKey, IndexLeafPage> leafPages;
    private IndexLeafPage lastLeafPage;
    private int id;

    public IndexStore(Path indexFilePath) {
        this.leafPages = new BTree<>();
    }

    public void addIndex(long documentIndex, int allocationMapIndex, int pageId, int offset) {
        if (leafPages.isEmpty() || !lastLeafPage.canAllocateOneMore()) {
            lastLeafPage = new IndexLeafPage(id);
            leafPages.add(new IndexKey(id, documentIndex), lastLeafPage);
            id++;
        }

        lastLeafPage.addIndex(documentIndex, allocationMapIndex, pageId, offset);
    }

    public IndexLeafPage.Index findIndexPage(long documentIndex) {
        return leafPages.get(new IndexKey(-1, documentIndex)).getIndex(documentIndex);
    }
}
