package filestorage.domain.search;

public class FindById implements Find {
    private final String key;
    private final long value;

    public FindById(String key, long value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public long getValue() {
        return value;
    }
}