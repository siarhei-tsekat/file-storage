package filestorage.internals;

import filestorage.internals.core.AllocationMap;
import filestorage.internals.core.MappingPage;
import filestorage.internals.core.Page;
import filestorage.utils.StoreIO;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class MappingStore {
    private final List<AllocationMap> allocationMaps = new ArrayList<>();
    private final Path mappingFilePath;

    public MappingStore(Path mappingFilePath) {
        this.mappingFilePath = mappingFilePath;

        long lengthBytes = StoreIO.getFileLength(mappingFilePath);

        if (lengthBytes > 0) {
            long countOfAllocationMaps = lengthBytes > Page.AllocationSector ? ((lengthBytes / Page.AllocationSector) + (lengthBytes % Page.AllocationSector == 0 ? 0 : 1)) : 1;

            int allocationMapIndex = 0;

            while (countOfAllocationMaps-- > 0) {

                long mapFilePosition = AllocationMap.calculateAllocationMapFilePosition(allocationMapIndex);

                byte[] bytes = StoreIO.readFromFile(mappingFilePath, mapFilePosition, Page.Size_Bt);
                allocationMaps.add(AllocationMap.fromBytes(allocationMapIndex, bytes));

                allocationMapIndex++;
            }
        }

        if (allocationMaps.isEmpty()) {
            allocationMaps.add(new AllocationMap(0));
        }
    }

    public MappingPage getNewPage(int allocationDataMapIndex) {

        AllocationMap allocationMap = allocationMaps.get(allocationMaps.size() - 1);

        if (allocationMap.canAllocateNewPage()) {

            int pageId = allocationMap.getAndAllocateNextAvailablePageId();
            long pageFileLocation = AllocationMap.calculatePageFilePosition(allocationMap.getAllocationMapIndex(), pageId);

            return new MappingPage(allocationMap.getAllocationMapIndex(), pageId, allocationDataMapIndex, pageFileLocation);
        }
        else {

            AllocationMap newAllocationMap = new AllocationMap(allocationMap.getAllocationMapIndex() + 1);
            allocationMaps.add(newAllocationMap);

            int pageId = newAllocationMap.getAndAllocateNextAvailablePageId();
            long pageFileLocation = AllocationMap.calculatePageFilePosition(newAllocationMap.getAllocationMapIndex(), pageId);

            return new MappingPage(newAllocationMap.getAllocationMapIndex(), pageId, allocationDataMapIndex, pageFileLocation);
        }
    }

    public List<AllocationMap> getAllocationMaps() {
        return allocationMaps;
    }

    public MappingPage getMappingPage(int allocationMapIndex, int mappingPageId) {
        long filePosition = AllocationMap.calculatePageFilePosition(allocationMapIndex, mappingPageId);
        byte[] bytes = StoreIO.readFromFile(mappingFilePath, filePosition, Page.Size_Bt);
        return MappingPage.fromBytes(allocationMapIndex, mappingPageId, filePosition, bytes);
    }
}