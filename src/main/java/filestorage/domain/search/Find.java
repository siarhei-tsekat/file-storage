package filestorage.domain.search;

public interface Find {

    static FindById byId(long id) {
        return new FindById("id", id);
    }
}
