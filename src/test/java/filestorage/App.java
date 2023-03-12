package filestorage;

import filestorage.domain.Store;
import filestorage.domain.StoreCollection;
import filestorage.domain.StoreDocument;

public class App {
    public static void main(String[] args) {
        Store store = new Store("demo-db");
        StoreDocument storeDocument = new StoreDocument(System.nanoTime());
        storeDocument.put("name", "bob");

        StoreCollection collection = store.getCollection("collection");
        collection.insert(storeDocument);

        store.commit(collection);
        store.close();
    }
}
