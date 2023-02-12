package filestorage.domain.search;

public interface Find {

    static FindById byId(int id) {
        return new FindById("id", id);
    }
}
