package filestorage.domain;

import filestorage.internals.DataStore;
import filestorage.internals.IndexStore;
import filestorage.internals.MappingStore;
import filestorage.domain.search.Find;
import filestorage.domain.search.FindById;
import filestorage.internals.core.CollectionAllocationPage;
import filestorage.internals.core.DataPage;
import filestorage.internals.core.DataRow;
import filestorage.internals.core.IndexLeafPage;
import filestorage.internals.core.MappingPage;
import filestorage.internals.core.MappingPageInfo;
import filestorage.internals.core.Page;
import filestorage.internals.StoreMetaInfo;
import filestorage.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class StoreCollection {

    private final HashMap<Long, StoreDocument> transientDocuments = new HashMap<>();
    private final CollectionName collectionName;
    private final DataStore dataStore;
    private final MappingStore mappingStore;
    private final IndexStore indexStore;
    private final StoreMetaInfo storeMetaInfo;
    private final HashMap<Integer, MappingPage> mappingPages = new HashMap<>();
    private CollectionAllocationPage collectionAllocationPage;

    public StoreCollection(
            CollectionName collectionName,
            DataStore dataStore,
            MappingStore mappingStore,
            StoreMetaInfo storeMetaInfo,
            IndexStore indexStore,
            CollectionAllocationPage collectionAllocationPage
    ) {
        this.collectionName = collectionName;
        this.dataStore = dataStore;
        this.mappingStore = mappingStore;
        this.storeMetaInfo = storeMetaInfo;
        this.indexStore = indexStore;
        this.collectionAllocationPage = collectionAllocationPage;

        collectionAllocationPage.getMappingPageInfoList()
                .stream()
                .map(mappingPageInfo -> mappingStore.getMappingPage(mappingPageInfo.getAllocationMapIndex(), mappingPageInfo.getMappingPageId()))
                .forEach(mp -> this.mappingPages.put(mp.getAllocationMapIndex(), mp));
    }

    public StoreCollection(CollectionName collectionName, DataStore dataStore, MappingStore mappingStore, IndexStore indexStore, StoreMetaInfo storeMetaInfo) {
        this.collectionName = collectionName;
        this.dataStore = dataStore;
        this.mappingStore = mappingStore;
        this.indexStore = indexStore;
        this.storeMetaInfo = storeMetaInfo;

        MappingPage mappingPage = mappingStore.getNewPage(dataStore.getAvailableAllocationMapIndex());

        collectionAllocationPage = new CollectionAllocationPage(storeMetaInfo.getAllocationPages().size(), collectionName, mappingPage);
        storeMetaInfo.addCollectionAllocationPage(collectionAllocationPage);

        mappingPages.put(mappingPage.getAllocationMapIndex(), mappingPage);
    }

    public StoreDocument findOne(Find find) {

        if (find instanceof FindById) {
            FindById findById = (FindById) find;
            String key = findById.getKey();
            int idValue = findById.getValue();
            IndexLeafPage.Index index = indexStore.findIndexPage(idValue);
            int dataPageId = index.getDataPageId();
            int dataAllocationMapId = index.getDataAllocationMapIndex();
            int offset = index.getOffset();
            DataPage dataPage = dataStore.getDataPage(dataAllocationMapId, dataPageId);
            DataRow dataRow = dataPage.getDataRowByOffset(offset); // ??
            // read all data rows and find storeDocument

            return StoreDocument.fromBytes(dataRow.getData());
        }
        else {
            return new StoreDocument(UUID.randomUUID());
        }
    }

    public void insert(StoreDocument storeDocument) {
        transientDocuments.put(storeDocument.getId(), storeDocument);
    }

    public void insert(List<StoreDocument> storeDocument) {
        storeDocument.forEach(doc -> transientDocuments.put(doc.getId(), doc));
    }

    HashMap<Long, StoreDocument> getTransientDocuments() {
        return transientDocuments;
    }

    void addDataPage(DataPage dataPage) {
        int pageId = dataPage.getPageId();
        int pamIndex = dataPage.getAllocationMapIndex();

        if (!mappingPages.containsKey(pamIndex)) {
            MappingPage newPage = mappingStore.getNewPage(pamIndex);
            mappingPages.put(pamIndex, newPage);
            collectionAllocationPage.getMappingPageInfoList().add(new MappingPageInfo(newPage.getAllocationMapIndex(), newPage.getPageId()));
        }
        mappingPages.get(pamIndex).markPage(pageId);
    }

    CollectionAllocationPage getCollectionAllocationPage() {
        return collectionAllocationPage;
    }

    HashMap<Integer, MappingPage> getMappingPages() {
        return mappingPages;
    }

    public Iterator<StoreDocument> iterator() {
        return new StoreDocumentIterator(new ArrayList<>(mappingPages.values()));
    }

    public String getName() {
        return collectionName.toString();
    }

    public long size() {
        return transientDocuments.size();
    }

    class StoreDocumentIterator implements Iterator<StoreDocument> {

        private final List<MappingPage> mappingPages;
        private MappingPage.MappingPageIterator dataPageIdIterator;
        private int mappingPageIndex = 0;
        private int storeDocumentsIndex = 0;
        private List<StoreDocument> storeDocuments = new ArrayList<>();

        public StoreDocumentIterator(List<MappingPage> mappingPages) {
            this.mappingPages = mappingPages;
            this.dataPageIdIterator = mappingPages.get(mappingPageIndex).getIterator();
        }

        @Override
        public boolean hasNext() {

            if (storeDocumentsIndex < storeDocuments.size() || dataPageIdIterator.hasNext()) {
                return true;
            }
            else if (mappingPageIndex + 1 < mappingPages.size()) {
                dataPageIdIterator = mappingPages.get(++mappingPageIndex).getIterator();
                storeDocumentsIndex = 0;
                return hasNext();
            }
            else {
                return false;
            }
        }

        @Override
        public StoreDocument next() {

            if (storeDocumentsIndex >= storeDocuments.size()) {

                storeDocuments = new ArrayList<>();

                Integer dataPadeId = dataPageIdIterator.next();
                int allocationMapIndex = mappingPages.get(mappingPageIndex).getAllocationMapIndex();
                DataPage dataPage = dataStore.getDataPage(allocationMapIndex, dataPadeId);

                if (dataPage.getPageType() == Page.PageType.IN_ROW) {

                    for (DataRow dataRow : dataPage.getDataRows()) {
                        storeDocuments.add(StoreDocument.fromBytes(dataRow.getData()));
                    }
                }
                else if (dataPage.getPageType() == Page.PageType.ROW_OVERFLOW) {

                    List<byte[]> bytes = new ArrayList<>();

                    bytes.add(dataPage.getDataRows().get(0).toBytes());

                    int nextPageId = dataPage.getNextPageId();

                    while (nextPageId != -1) {
                        DataPage dataPageNext = dataStore.getDataPage(allocationMapIndex, nextPageId);
                        dataPageIdIterator.skip(nextPageId);
                        dataPageNext.getDataRows().forEach(dr -> bytes.add(dr.getData()));
                        nextPageId = dataPageNext.getNextPageId();
                    }

                    storeDocuments.add(StoreDocument.fromBytes(Utils.asOne(bytes)));
                }

                storeDocumentsIndex = 0;
            }

            return storeDocuments.get(storeDocumentsIndex++);
        }
    }
}

