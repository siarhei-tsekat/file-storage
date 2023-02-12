package filestorage;

import filestorage.internals.core.AllocationMap;
import filestorage.internals.core.DataPage;
import filestorage.internals.core.DataRow;
import filestorage.internals.core.Page;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;

public class FileStorage_DataPage_Tests {

    @ParameterizedTest
    @CsvSource({"0,0", "0,100", "0,1000", "1,0", "1,100", "1,1000", "15,0", "15,1", "15,15"})
    public void cover_init(int allocationMapIndex, int pageId) {

        long pageFilePosition = AllocationMap.calculatePageFilePosition(allocationMapIndex, pageId);

        DataPage dataPage = new DataPage(allocationMapIndex, pageId, pageFilePosition, Page.PageType.IN_ROW);

        Assertions.assertEquals(allocationMapIndex, dataPage.getAllocationMapIndex());
        Assertions.assertEquals(pageId, dataPage.getPageId());
        Assertions.assertEquals(pageFilePosition, dataPage.filePosition());
        Assertions.assertEquals(Page.Size_Bt - Page.PageHeader_Size_Bt, dataPage.getFreeSpace());
        Assertions.assertTrue(dataPage.checkIfFit(dataPage.getFreeSpace() - DataPage.offsetLength));

        DataPage.PageHeader pageHeader = dataPage.getPageHeader();

        Assertions.assertEquals(allocationMapIndex, pageHeader.allocationMapIndex);
        Assertions.assertEquals(pageId, pageHeader.pageId);
        Assertions.assertEquals(pageFilePosition, pageHeader.filePosition);
        Assertions.assertEquals(0, pageHeader.numberOfRows);
        Assertions.assertEquals(Page.Size_Bt - Page.PageHeader_Size_Bt, pageHeader.freeSpace);
        Assertions.assertEquals(Page.PageHeader_Size_Bt, pageHeader.nextAvailablePosition);
    }

    @ParameterizedTest
    @CsvSource({"0,0", "0,100", "0,1000", "1,0", "1,100", "1,1000", "15,0", "15,1", "15,15"})
    public void cover_addData(int allocationMapIndex, int pageId) {
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);

        long fileLocation = AllocationMap.calculatePageFilePosition(allocationMapIndex, pageId);

        DataPage dataPage = new DataPage(allocationMapIndex, pageId, fileLocation, Page.PageType.IN_ROW);

        dataPage.addRow(data);

        Assertions.assertTrue(dataPage.checkIfFit(data.length));
        Assertions.assertEquals(Page.Size_Bt - Page.PageHeader_Size_Bt - DataPage.offsetLength - data.length, dataPage.getFreeSpace());

        Assertions.assertEquals(1, dataPage.getOffsets().size());
        Assertions.assertEquals(Page.PageHeader_Size_Bt, dataPage.getOffsets().get(0));

        DataPage.PageHeader pageHeader = dataPage.getPageHeader();
        Assertions.assertEquals(1, pageHeader.numberOfRows);
        Assertions.assertEquals(Page.Size_Bt - Page.PageHeader_Size_Bt - DataPage.offsetLength - data.length, pageHeader.freeSpace);
        Assertions.assertEquals(Page.PageHeader_Size_Bt + data.length, pageHeader.nextAvailablePosition);

        dataPage.addRow(data);

        Assertions.assertTrue(dataPage.checkIfFit(data.length));
        Assertions.assertEquals(Page.Size_Bt - Page.PageHeader_Size_Bt - (DataPage.offsetLength * 2) - (data.length * 2), dataPage.getFreeSpace());

        Assertions.assertEquals(2, dataPage.getOffsets().size());
        Assertions.assertEquals(Page.PageHeader_Size_Bt + data.length, dataPage.getOffsets().get(1));

        Assertions.assertEquals(2, pageHeader.numberOfRows);
        Assertions.assertEquals(Page.Size_Bt - Page.PageHeader_Size_Bt - (DataPage.offsetLength * 2) - (data.length * 2), pageHeader.freeSpace);
        Assertions.assertEquals(Page.PageHeader_Size_Bt + (data.length * 2), pageHeader.nextAvailablePosition);
    }

    @ParameterizedTest
    @CsvSource({"0,0", "0,100", "0,1000", "1,0", "1,100", "1,1000", "15,0", "15,1", "15,15"})
    public void cover_toBytes_FromBytes(int allocationMapIndex, int pageId) {
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);

        long fileLocation = AllocationMap.calculatePageFilePosition(allocationMapIndex, pageId);

        DataPage dataPage = new DataPage(allocationMapIndex, pageId, fileLocation, Page.PageType.IN_ROW);
        dataPage.addRow(data);
        byte[] bytes = dataPage.toBytes();

        DataPage dataPageDes = DataPage.fromByte(bytes);

        Assertions.assertEquals(dataPage.getPageId(), dataPageDes.getPageId());
        Assertions.assertEquals(dataPage.getAllocationMapIndex(), dataPageDes.getAllocationMapIndex());
        Assertions.assertEquals(dataPage.getFreeSpace(), dataPageDes.getFreeSpace());
        Assertions.assertEquals(dataPage.getDataRows().size(), dataPageDes.getDataRows().size());
        Assertions.assertEquals(dataPage.getPageHeader().pageId, dataPageDes.getPageHeader().pageId);
        Assertions.assertEquals(dataPage.getPageHeader().allocationMapIndex, dataPageDes.getPageHeader().allocationMapIndex);
        Assertions.assertEquals(dataPage.getPageHeader().filePosition, dataPageDes.getPageHeader().filePosition);
        Assertions.assertEquals(dataPage.getPageHeader().numberOfRows, dataPageDes.getPageHeader().numberOfRows);
        Assertions.assertEquals(dataPage.getPageHeader().freeSpace, dataPageDes.getPageHeader().freeSpace);
        Assertions.assertEquals(dataPage.getPageHeader().nextAvailablePosition, dataPageDes.getPageHeader().nextAvailablePosition);

        byte[] data1 = dataPageDes.getDataRows().get(0).getData();

        Assertions.assertArrayEquals(data, data1);
    }

    @ParameterizedTest
    @CsvSource({"0,0", "0,100", "0,1000", "1,0", "1,100", "1,1000", "15,0", "15,1", "15,15"})
    public void cover_FillFullyWithSmallData(int allocationMapIndex, int pageId) {

        long fileLocation = AllocationMap.calculatePageFilePosition(allocationMapIndex, pageId);

        DataPage dataPage = new DataPage(allocationMapIndex, pageId, fileLocation, Page.PageType.IN_ROW);

        int rows = 0;

        while (true) {

            String s = rows + "hello";

            byte[] data = s.getBytes(StandardCharsets.UTF_8);

            if (dataPage.checkIfFit(data.length)) {
                dataPage.addRow(data);
                rows++;
            }
            else {
                break;
            }
        }

        Assertions.assertEquals(rows, dataPage.getDataRows().size());
        Assertions.assertEquals(rows, dataPage.getPageHeader().numberOfRows);

        byte[] bytes = dataPage.toBytes();

        DataPage dataPageDes = DataPage.fromByte(bytes);

        Assertions.assertEquals(dataPage.getFreeSpace(), dataPageDes.getFreeSpace());
        Assertions.assertEquals(dataPage.getDataRows().size(), dataPageDes.getDataRows().size());
        Assertions.assertEquals(dataPage.getPageHeader().pageId, dataPageDes.getPageHeader().pageId);
        Assertions.assertEquals(dataPage.getPageHeader().allocationMapIndex, dataPageDes.getPageHeader().allocationMapIndex);
        Assertions.assertEquals(dataPage.getPageHeader().filePosition, dataPageDes.getPageHeader().filePosition);
        Assertions.assertEquals(dataPage.getPageHeader().numberOfRows, dataPageDes.getPageHeader().numberOfRows);
        Assertions.assertEquals(dataPage.getPageHeader().freeSpace, dataPageDes.getPageHeader().freeSpace);
        Assertions.assertEquals(dataPage.getPageHeader().nextAvailablePosition, dataPageDes.getPageHeader().nextAvailablePosition);

        int row = 0;

        for (DataRow dataRow : dataPageDes.getDataRows()) {
            String s = row + "hello";
            byte[] data = s.getBytes(StandardCharsets.UTF_8);

            Assertions.assertArrayEquals(data, dataRow.getData());
            row++;
        }
    }

    @ParameterizedTest
    @CsvSource({"0,0", "0,100", "0,1000", "1,0", "1,100", "1,1000", "15,0", "15,1", "15,15"})
    public void cover_FillFullyWithBigData(int allocationMapIndex, int pageId) {
        long fileLocation = AllocationMap.calculatePageFilePosition(allocationMapIndex, pageId);

        String data = generate8KBString();
        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);

        DataPage dataPage = new DataPage(allocationMapIndex, pageId, fileLocation, Page.PageType.IN_ROW);

        Assertions.assertTrue(dataPage.checkIfFit(dataBytes.length));
        dataPage.addRow(dataBytes);

        byte[] bytes = dataPage.toBytes();

        DataPage dataPageDes = DataPage.fromByte(bytes);

        Assertions.assertEquals(dataPage.getFreeSpace(), dataPageDes.getFreeSpace());
        Assertions.assertEquals(dataPage.getDataRows().size(), dataPageDes.getDataRows().size());
        Assertions.assertEquals(dataPage.getPageHeader().pageId, dataPageDes.getPageHeader().pageId);
        Assertions.assertEquals(dataPage.getPageHeader().allocationMapIndex, dataPageDes.getPageHeader().allocationMapIndex);
        Assertions.assertEquals(dataPage.getPageHeader().filePosition, dataPageDes.getPageHeader().filePosition);
        Assertions.assertEquals(dataPage.getPageHeader().numberOfRows, dataPageDes.getPageHeader().numberOfRows);
        Assertions.assertEquals(dataPage.getPageHeader().freeSpace, dataPageDes.getPageHeader().freeSpace);
        Assertions.assertEquals(dataPage.getPageHeader().nextAvailablePosition, dataPageDes.getPageHeader().nextAvailablePosition);

        byte[] data1 = dataPageDes.getDataRows().get(0).getData();

        Assertions.assertArrayEquals(dataBytes, data1);
    }

    private String generate8KBString() {
        StringBuilder sb = new StringBuilder();
        int limitBytes = (Page.Size_Bt) - 4 - Page.PageHeader_Size_Bt;

        while (sb.length() < limitBytes) {
            sb.append("a");
        }
        return sb.toString();
    }
}