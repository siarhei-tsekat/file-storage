package filestorage.domain.search;

public class FindById implements Find {
    private final String key;
    private final int value;

    public FindById(String key, int value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }
}