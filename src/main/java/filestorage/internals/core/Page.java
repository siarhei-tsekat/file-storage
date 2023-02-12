package filestorage.internals.core;

public interface Page {

    public static final int Size_Bt = 8 * 1024;
    public static final int PageHeader_Size_Bt = 192;
    public static final int AllocationSector = Page.Size_Bt * (BitMap.Bitmap_Size_Bt + 1);
    public static final int FreeSpaceDefault = Page.Size_Bt - PageHeader_Size_Bt - 4;

    public enum PageType {
        ROW_OVERFLOW,
        IN_ROW;

        public static PageType fromInt(int pageType) {
            return pageType == 0 ? ROW_OVERFLOW : IN_ROW;
        }

        public int toInt() {
            return this == ROW_OVERFLOW ? 0 : 1;
        }
    }

    byte[] toBytes();

    long filePosition();
}
