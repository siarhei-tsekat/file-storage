package filestorage;

import filestorage.internals.core.BitMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Iterator;

public class FileStorage_BitMap_Tests {

    @ParameterizedTest
    @ValueSource(ints = {8, 1_000, 8_000, 64_000, BitMap.Bitmap_Size_Bt})
    public void cover_set_unset_size_getNextFree(int number) {

        BitMap bitMap = new BitMap(number);

        Assertions.assertEquals(number, bitMap.size());
        Assertions.assertEquals(0, bitMap.getNextFree());

        bitMap.set(0);
        Assertions.assertEquals(1, bitMap.getNextFree());

        bitMap.unset(0);
        Assertions.assertEquals(0, bitMap.getNextFree());

        bitMap.set(number - 1);
        Assertions.assertEquals(1, bitMap.getNextFree());

        bitMap.unset(number - 1);
        Assertions.assertEquals(2, bitMap.getNextFree());

        bitMap.unset(0);
        bitMap.unset(1);
        bitMap.unset(2);

        for (int i = 0; i < number - 1; i++) {
            bitMap.set(i);
            Assertions.assertEquals(i + 1, bitMap.getNextFree());
        }
        bitMap.set(number - 1);
        Assertions.assertEquals(-1, bitMap.getNextFree());
    }

    @ParameterizedTest
    @ValueSource(ints = {8, 64_000, 1_000, 8_000, BitMap.Bitmap_Size_Bt})
    public void cover_toBytes_fromBytes(int number) {
        BitMap bitMap = new BitMap(number);

        bitMap.set(0);
        bitMap.set(number - 1);

        byte[] bytes = bitMap.toBytes();

        BitMap desirelized = BitMap.fromBytes(bytes);

        Assertions.assertFalse(desirelized.isFree(0));
        Assertions.assertFalse(desirelized.isFree(number - 1));

        Assertions.assertEquals(1, desirelized.getNextFree());
    }

    @ParameterizedTest
    @ValueSource(ints = {8, 64_000, 1_000, 8_000, BitMap.Bitmap_Size_Bt})
    public void cover_iterator(int number) {
        BitMap bitMap = new BitMap(number);

        Iterator<BitMap.BitMapPair> iterator = bitMap.iterator();
        int total = 0;

        while (iterator.hasNext()) {
            iterator.next();
            total++;
        }

        Assertions.assertEquals(number, total);

        bitMap.set(0);
        bitMap.set(number / 2);
        bitMap.set(number - 1);

        int total2 = 0;

        while (iterator.hasNext()) {
            BitMap.BitMapPair res = iterator.next();

            if (total2 == 0) {
                Assertions.assertEquals(0, res.ind);
                Assertions.assertEquals(BitMap._allocated, res.value);
            }
            else if (total2 == number / 2) {
                Assertions.assertEquals(number / 2, res.ind);
                Assertions.assertEquals(BitMap._allocated, res.value);
            }
            else if (total2 == number - 1) {
                Assertions.assertEquals(number - 1, res.ind);
                Assertions.assertEquals(BitMap._allocated, res.value);
            }
            else {
                Assertions.assertEquals(total2, res.ind);
                Assertions.assertEquals(BitMap._free, res.value);
            }
            total2++;
        }
    }
}

