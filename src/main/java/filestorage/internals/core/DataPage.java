package filestorage.internals.core;

import filestorage.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataPage implements Page {
    public static final int offsetLength = 4; // length of each row offset (length of an int = 4 bytes)

    private final PageHeader header;
    private final List<DataRow> rows = new ArrayList<>();
    private final List<Integer> offsets = new ArrayList<>();

    public DataPage(int allocationMapIndex, int pageId, long fileLocation, Page.PageType pageType) {
        this.header = new PageHeader(allocationMapIndex, pageId, fileLocation, Page.PageHeader_Size_Bt, Page.Size_Bt - Page.PageHeader_Size_Bt, 0, pageType);
    }

    private DataPage(PageHeader pageHeader) {
        this.header = pageHeader;
    }

    public int getPageId() {
        return header.pageId;
    }

    public int getAllocationMapIndex() {
        return header.allocationMapIndex;
    }

    public void addRow(byte[] data) {
        if (checkIfFit(data.length) == false) {
            throw new RuntimeException("Data is to big to be kept in Page. Free space: " + getFreeSpace() + " . Data length: " + data.length);
        }
        rows.add(new DataRow(data));
        offsets.add(header.nextAvailablePosition);
        header.numberOfRows++;
        header.freeSpace -= data.length + offsetLength;
        header.nextAvailablePosition += data.length;
    }

    private void addRowRestored(byte[] data, int offset) {
        rows.add(new DataRow(data));
        offsets.add(offset);
    }

    public List<DataRow> getDataRows() {
        return rows;
    }

    public byte[] toBytes() {
        byte[] all = new byte[Page.Size_Bt];

        int i = 0;

        for (byte b : header.toByte()) {
            all[i++] = b;
        }

        for (byte b : getRowsAsByte()) {
            all[i++] = b;
        }

        int end = all.length - 1;

        byte[] offsetsAsByte = getOffsetsAsByte();

        for (int x = offsetsAsByte.length - 1; x >= 0; x--) {
            all[end--] = offsetsAsByte[x];
        }

        return all;
    }

    public long filePosition() {
        return header.filePosition;
    }

    public boolean checkIfFit(long length) {
        return header.freeSpace - offsetLength >= length;
    }

    public long getFreeSpace() {
        return header.freeSpace;
    }

    private byte[] getOffsetsAsByte() {
        byte[] res = new byte[offsets.size() * 4];
        int i = 0;

        for (int x = offsets.size() - 1; x >= 0; x--) {
            int offset = offsets.get(x);
            byte[] toByte = Utils.intToByte(offset);

            for (int j = 0; j < 4; j++) {
                res[i++] = toByte[j];
            }
        }

        return res;
    }

    private int getNextAvailablePosition() {
        return header.nextAvailablePosition;
    }

    private byte[] getRowsAsByte() {
        int total = 0;
        List<byte[]> bytes = new ArrayList<>();

        for (DataRow row : rows) {
            byte[] rowByte = row.toBytes();
            bytes.add(rowByte);
            total += rowByte.length;
        }

        byte[] res = new byte[total];

        int i = 0;

        for (byte[] aByte : bytes) {
            for (byte b : aByte) {
                res[i++] = b;
            }
        }

        return res;
    }

    public static DataPage fromByte(byte[] array) {

        PageHeader pageHeader = PageHeader.fromBytes(array);
        DataPage dataPage = new DataPage(pageHeader);
        int lastOffset = Utils.byteToInt(Arrays.copyOfRange(array, Page.Size_Bt - DataPage.offsetLength, Page.Size_Bt));

        int nextAvailable = dataPage.getNextAvailablePosition();

        for (int i = 0; i < pageHeader.numberOfRows - 1; i++) {

            int from = Page.Size_Bt - (i * DataPage.offsetLength);
            int to = from - DataPage.offsetLength;

            int rowOffset = Utils.byteToInt(Arrays.copyOfRange(array, to, from));
            int nextRowOffset = Utils.byteToInt(Arrays.copyOfRange(array, to - DataPage.offsetLength, to));
            lastOffset = nextRowOffset;
            byte[] data = Arrays.copyOfRange(array, rowOffset, nextRowOffset);
            dataPage.addRowRestored(data, lastOffset);
        }

        dataPage.addRowRestored(Arrays.copyOfRange(array, lastOffset, nextAvailable), lastOffset);

        return dataPage;
    }

    public PageHeader getPageHeader() {
        return header;
    }

    public List<Integer> getOffsets() {
        return offsets;
    }

    public void setNextPageId(int dataPageId) {
        header.nextPageId = dataPageId;
    }

    public PageType getPageType() {
        return header.pageType;
    }

    public int getNextPageId() {
        return header.nextPageId;
    }

    public int getLastRowOffset() {
        return offsets.get(offsets.size() - 1);
    }

    public DataRow getDataRowByOffset(int offset) {
        for (int i = 0; i < offsets.size(); i++) {
            if(offsets.get(i) == offset) {
                return rows.get(i);
            }
        }
        return null;
    }

    public static class PageHeader {

        public final int allocationMapIndex;
        public final long filePosition;
        public final int pageId;
        public int freeSpace;
        public int nextAvailablePosition;
        public int numberOfRows;
        private PageType pageType;
        public int nextPageId = -1;

        public PageHeader(int allocationMapIndex, int pageId, long filePosition, int nextAvailablePosition, int freeSpace, int numberOfRows, Page.PageType pageType) {
            this.pageId = pageId;
            this.allocationMapIndex = allocationMapIndex;
            this.filePosition = filePosition;
            this.nextAvailablePosition = nextAvailablePosition;
            this.freeSpace = freeSpace;
            this.numberOfRows = numberOfRows;
            this.pageType = pageType;
        }

        public byte[] toByte() {

            byte[] positionInFileBytes = Utils.longToByte(filePosition);
            byte[] pageIdBytes = Utils.intToByte(pageId);
            byte[] allocationMapIndexBytes = Utils.intToByte(allocationMapIndex);
            byte[] freeSpaceBytes = Utils.intToByte(freeSpace);
            byte[] nextAvailablePositionBytes = Utils.intToByte(nextAvailablePosition);
            byte[] numberOfRowsBytes = Utils.intToByte(numberOfRows);
            byte[] pageTypeBytes = Utils.intToByte(pageType.toInt());
            byte[] nextPageIdBytes = Utils.intToByte(nextPageId);

            return Utils.asOne(Page.PageHeader_Size_Bt,
                    positionInFileBytes,
                    pageIdBytes,
                    allocationMapIndexBytes,
                    freeSpaceBytes,
                    nextAvailablePositionBytes,
                    numberOfRowsBytes,
                    pageTypeBytes,
                    nextPageIdBytes
            );
        }

        public static PageHeader fromBytes(byte[] arr) {

            long positionInFile = Utils.byteToLong(Arrays.copyOfRange(arr, 0, 8));
            int pageId = Utils.byteToInt(Arrays.copyOfRange(arr, 8, 12));
            int allocationMapIndex = Utils.byteToInt(Arrays.copyOfRange(arr, 12, 16));
            int freeSpace = Utils.byteToInt(Arrays.copyOfRange(arr, 16, 20));
            int nextAvailablePosition = Utils.byteToInt(Arrays.copyOfRange(arr, 20, 24));
            int numberOfRows = Utils.byteToInt(Arrays.copyOfRange(arr, 24, 28));
            int pageType = Utils.byteToInt(Arrays.copyOfRange(arr, 28, 32));
            int nextPageId = Utils.byteToInt(Arrays.copyOfRange(arr, 32, 36));

            PageHeader pageHeader = new PageHeader(allocationMapIndex,
                    pageId,
                    positionInFile,
                    nextAvailablePosition,
                    freeSpace,
                    numberOfRows,
                    PageType.fromInt(pageType)
            );

            pageHeader.nextPageId = nextPageId;

            return pageHeader;
        }
    }
}
