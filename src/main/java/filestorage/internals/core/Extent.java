package filestorage.internals.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Extent {

    private final List<DataPage> pages = new ArrayList<>();
    private final int allocationMapIndex;
    private final List<Integer> pageIds;

    public Extent(int allocationMapIndex, List<Integer> pageIds) {
        this.allocationMapIndex = allocationMapIndex;
        this.pageIds = pageIds;
    }

    public List<DataPage> getDataPages() {
        return pages;
    }

    public void allocate(byte[] bytes) {
        int incomingByteLength = bytes.length;
        int toAllocate = bytes.length;

        for (int i = 0; i < pageIds.size(); i++) {

            Integer pageId = pageIds.get(i);
            DataPage dataPage = new DataPage(allocationMapIndex, pageId, AllocationMap.calculatePageFilePosition(allocationMapIndex, pageId), Page.PageType.ROW_OVERFLOW);

            if (i + 1 < pageIds.size()) {
                dataPage.setNextPageId(pageIds.get(i + 1));
            }

            byte[] b1 = Arrays.copyOfRange(bytes, 0, Math.min(bytes.length, Page.Size_Bt - Page.PageHeader_Size_Bt - 4));
            dataPage.addRow(b1);

            toAllocate -= b1.length;

            if (b1.length < bytes.length) {
                bytes = Arrays.copyOfRange(bytes, b1.length, bytes.length);
            }
            pages.add(dataPage);
        }

        if (toAllocate > 0) {
            throw new RuntimeException("Not all bytes were allocated to pages. Remaings: " + toAllocate + ", incomingByteLength: " + incomingByteLength);
        }
    }

    public DataPage getRootPage() {
        return pages.get(0);
    }
}
