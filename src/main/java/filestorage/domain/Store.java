package filestorage.domain;

import filestorage.internals.DataStore;
import filestorage.internals.IndexStore;
import filestorage.internals.MappingStore;
import filestorage.internals.core.CollectionAllocationPage;
import filestorage.internals.core.DataPage;
import filestorage.internals.core.Extent;
import filestorage.internals.core.Page;
import filestorage.internals.StoreMetaInfo;
import filestorage.utils.StoreIO;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Store implements Closeable {
    private final Path storeMetaInfoFilePath;
    private final Path dataFilePath;
    private final Path mappingFilePath;
    private final Path indexFilePath;
    private final HashMap<CollectionName, StoreCollection> storeCollections = new HashMap<>();
    private final StoreMetaInfo storeMetaInfo;
    private final DataStore dataStore;
    private final MappingStore mappingStore;
    private final IndexStore indexStore;

    public Store(String storeName) {

        this.storeMetaInfoFilePath = getStoreMetaInfoFilePath(storeName);
        this.dataFilePath = getDataFilePath(storeName);
        this.mappingFilePath = getMappingFilePath(storeName);
        this.indexFilePath = getIndexFilePath(storeName);

        createFileIfNotExists(storeMetaInfoFilePath);
        createFileIfNotExists(dataFilePath);
        createFileIfNotExists(mappingFilePath);

        storeMetaInfo = new StoreMetaInfo(storeMetaInfoFilePath);
        dataStore = new DataStore(dataFilePath);
        mappingStore = new MappingStore(mappingFilePath);
        indexStore = new IndexStore(indexFilePath);

        for (CollectionAllocationPage collectionAllocationPage : storeMetaInfo.getAllCollectionAllocationInfo()) {

            CollectionName collectionName = collectionAllocationPage.getCollectionName();

            StoreCollection storeCollection = new StoreCollection(collectionName, dataStore, mappingStore, storeMetaInfo, indexStore, collectionAllocationPage);
            storeCollections.put(collectionName, storeCollection);
        }
    }

    public void close() {
        StoreIO.writeToFile(dataFilePath.toString(), dataStore.getAllocationMaps().stream());
        StoreIO.writeToFile(mappingFilePath.toString(), mappingStore.getAllocationMaps().stream());
        StoreIO.writeToFile(storeMetaInfoFilePath.toString(), storeMetaInfo.toBytes(), 0);
    }

    public StoreCollection getCollection(String name) {

        CollectionName collectionName = new CollectionName(name);

        StoreCollection storeCollection = storeCollections.get(collectionName);

        if (storeCollection == null) {

            StoreCollection newStoreCollection = new StoreCollection(collectionName, dataStore, mappingStore, indexStore, storeMetaInfo);
            storeCollections.put(collectionName, newStoreCollection);
        }
        return storeCollections.get(collectionName);
    }

    public Set<String> getCollectionNames() {
        return storeCollections.keySet().stream().map(cn -> cn.toString()).collect(Collectors.toSet());
    }

    String getStoreFilePath() {
        return storeMetaInfoFilePath.toString();
    }

    public void commit(StoreCollection storeCollection) {
        //todo:tsekot add lookup for the last free page, almost free
        HashMap<Long, StoreDocument> transientDocuments = storeCollection.getTransientDocuments();

        List<DataPage> all = new ArrayList<>();

        for (Map.Entry<Long, StoreDocument> entry : transientDocuments.entrySet()) {

            StoreDocument storeDocument = entry.getValue();
            byte[] bytes = storeDocument.toBytes();

            List<DataPage> dataPages = allocateStoreDocument(storeCollection, storeDocument.getId(), bytes);

            all.addAll(dataPages);
        }
        transientDocuments.clear();

        StoreIO.writeToFile(dataFilePath.toString(), all.stream());
        StoreIO.writeToFile(mappingFilePath.toString(), storeCollection.getMappingPages().values().stream());
    }

    private List<DataPage> allocateStoreDocument(StoreCollection storeCollection, long documentId, byte[] bytes) {

        List<DataPage> res = new ArrayList<>();

        if (bytes.length >= Page.Size_Bt - Page.PageHeader_Size_Bt - 4) {

            Extent extent = dataStore.getNewExtent(bytes.length);
            extent.allocate(bytes);
            List<DataPage> dataPages = extent.getDataPages();

            for (DataPage dataPage : dataPages) {

                storeCollection.addDataPage(dataPage);

                res.add(dataPage);
            }
            DataPage dataPage = extent.getRootPage();
            indexStore.addIndex(documentId, dataPage.getAllocationMapIndex(), dataPage.getPageId(), dataPage.getLastRowOffset());
        }
        else {

            DataPage dataPage = dataStore.getNewPage();
            storeCollection.addDataPage(dataPage);
            dataPage.addRow(bytes);
            res.add(dataPage);
            indexStore.addIndex(documentId, dataPage.getAllocationMapIndex(), dataPage.getPageId(), dataPage.getLastRowOffset());
        }

        return res;
    }

    private void createFileIfNotExists(Path filePath) {
        try {

            if (Files.exists(filePath) == false) {
                Files.createFile(filePath);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path getStoreMetaInfoFilePath(String storeName) {
        return Path.of(String.format("D:\\tmp\\%s_store_meta.txt", storeName));
    }

    private Path getDataFilePath(String storeName) {
        return Path.of(String.format("D:\\tmp\\%s_store_data.txt", storeName));
    }

    private Path getMappingFilePath(String storeName) {
        return Path.of(String.format("D:\\tmp\\%s_store_mapping.txt", storeName));
    }

    private Path getIndexFilePath(String storeName) {
        return Path.of(String.format("D:\\tmp\\%s_store_index.txt", storeName));
    }
}

