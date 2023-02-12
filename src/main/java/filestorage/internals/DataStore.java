package filestorage.internals;

import filestorage.internals.core.AllocationMap;
import filestorage.internals.core.DataPage;
import filestorage.internals.core.Extent;
import filestorage.internals.core.Page;
import filestorage.utils.StoreIO;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DataStore {

    private final List<AllocationMap> allocationMaps = new ArrayList<>();
    private final Path dataFilePath;

    public DataStore(Path dataFilePath) {
        this.dataFilePath = dataFilePath;

        long lengthBytes = StoreIO.getFileLength(dataFilePath);

        if (lengthBytes > 0) {
            long countOfAllocationMaps = lengthBytes > Page.AllocationSector ? ((lengthBytes / Page.AllocationSector) + (lengthBytes % Page.AllocationSector == 0 ? 0 : 1)) : 1;

            int allocationMapIndex = 0;

            while (countOfAllocationMaps-- > 0) {

                long mapFilePosition = AllocationMap.calculateAllocationMapFilePosition(allocationMapIndex);

                byte[] bytes = StoreIO.readFromFile(dataFilePath, mapFilePosition, Page.Size_Bt);
                allocationMaps.add(AllocationMap.fromBytes(allocationMapIndex, bytes));

                allocationMapIndex++;
            }
        }

        if (allocationMaps.isEmpty()) {
            allocationMaps.add(new AllocationMap(0));
        }
    }

    public DataPage getNewPage() {

        AllocationMap allocationMap = allocationMaps.get(allocationMaps.size() - 1);

        if (allocationMap.canAllocateNewPage()) {

            return getDataPage(allocationMap, Page.PageType.IN_ROW);
        }
        else {

            AllocationMap newAllocationMap = new AllocationMap(allocationMap.getAllocationMapIndex() + 1);
            allocationMaps.add(newAllocationMap);

            return getDataPage(newAllocationMap, Page.PageType.IN_ROW);
        }
    }

    public Extent getNewExtent(int length) {

        AllocationMap allocationMap = allocationMaps.get(allocationMaps.size() - 1);

        if (allocationMap.canAllocateNewExtent(length)) {

            return getExtent(allocationMap, length);
        }
        else {

            AllocationMap newAllocationMap = new AllocationMap(allocationMap.getAllocationMapIndex() + 1);
            allocationMaps.add(newAllocationMap);

            return getExtent(newAllocationMap, length);
        }
    }

    public List<AllocationMap> getAllocationMaps() {
        return allocationMaps;
    }

    public int getAvailableAllocationMapIndex() {
        int index = 0;
        int slots = 0;

        for (AllocationMap allocationMap : allocationMaps) {
            if (allocationMap.getAmountOfFreeSlots() > slots) {
                index = allocationMap.getAllocationMapIndex();
            }
        }
        return index;
    }

    public DataPage getDataPage(int allocationMapIndex, Integer dataPadeId) {

        long pageFileLocation = AllocationMap.calculatePageFilePosition(allocationMapIndex, dataPadeId);
        byte[] bytes = StoreIO.readFromFile(dataFilePath, pageFileLocation, Page.Size_Bt);
        return DataPage.fromByte(bytes);
    }

    private Extent getExtent(AllocationMap allocationMap, int length) {

        List<Integer> pageIds = allocationMap.getAndAllocateNextAvailablePageId(length);
        int allocationMapIndex = allocationMap.getAllocationMapIndex();
        return new Extent(allocationMapIndex, pageIds);
    }

    private DataPage getDataPage(AllocationMap allocationMap, Page.PageType pageType) {

        int pageId = allocationMap.getAndAllocateNextAvailablePageId();
        int allocationMapIndex = allocationMap.getAllocationMapIndex();

        long pageFileLocation = AllocationMap.calculatePageFilePosition(allocationMapIndex, pageId);
        return new DataPage(allocationMapIndex, pageId, pageFileLocation, pageType);
    }
}


