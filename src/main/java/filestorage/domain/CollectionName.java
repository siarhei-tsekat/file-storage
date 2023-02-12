package filestorage.domain;

import java.util.Objects;

public class CollectionName implements Comparable<CollectionName> {
    private final String collectionName;

    public CollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CollectionName that = (CollectionName) o;
        return Objects.equals(collectionName, that.collectionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(collectionName);
    }

    @Override
    public String toString() {
        return collectionName;
    }

    @Override
    public int compareTo(CollectionName that) {
        return this.collectionName.compareTo(that.collectionName);
    }
}
