package filestorage.internals.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BitMap {

    public static final int Bitmap_Size_Bt = 64_000;
    public static final boolean allocated = true;
    public static final boolean free = false;
    public static final int _free = 0;
    public static final int _allocated = 1;

    private final boolean[] bitmap;
    private int amountFree;

    /**
     * Size of BitMap MUST be multiple of 8 otherwise after deserialization the size of the bit map will be expanded to be multiple of 8.
     * An example. new BitMap(3).toBytes() -> byte [0,0,0,_,_,_,_,_] == 1 byte. Bitmap.fromBytes(byte[]) -> byte [0,0,0,_,_,_,_,_]
     */
    public BitMap(int size) {
        bitmap = new boolean[size];
        this.amountFree = size;
    }

    private BitMap(boolean[] bitmap, int amountFree) {
        this.bitmap = bitmap;
        this.amountFree = amountFree;
    }

    public void set(int ind) {
        bitmap[ind] = allocated;
        amountFree--;
    }

    public void unset(int ind) {
        bitmap[ind] = free;
        amountFree++;
        lastTaken = ind < lastTaken ? -1 : lastTaken;
    }

    public int getAmountFree() {
        return amountFree;
    }

    public int getMaxAmountFreeInRow() {
        int res = 0;
        int tmp = 0;

        for (int i = 0; i < bitmap.length; i++) {

            if (bitmap[i] == free) {
                tmp++;
            }
            else {
                res = Math.max(res, tmp);
                tmp = 0;
            }
        }

        return Math.max(res, tmp);
    }

    public List<Integer> getNextFreeInRow(long amountOfPages) {
        List<Integer> res = new ArrayList<>();

        for (int i = 0; i < bitmap.length; i++) {

            if (res.size() == amountOfPages) {
                return res;
            }
            if (bitmap[i] == free) {
                res.add(i);
            }
            else {
                res.clear();
            }
        }
        if (res.size() == amountOfPages) {
            return res;
        }
        else {
            throw new RuntimeException("Didn't manage to find free slots: " + amountOfPages + " going in row.");
        }
    }

    public int lastTaken = -1;

    //todo:tsekot sped it up -> high use of this method
    public int getNextFree() {

        for (int i = lastTaken + 1; i < bitmap.length; i++) {
            if (bitmap[i] == free) {
                lastTaken = i;
                return i;
            }
        }
        return -1;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(bitmap.length);

        for (int i = 0; i < bitmap.length; ) {
            sb.append("[");
            StringBuilder tmp = new StringBuilder();

            for (int j = 0; j < 8 && i < bitmap.length; j++, i++) {
                if (bitmap[i] == free) {
                    tmp.append(_free);
                }
                else {
                    tmp.append(_allocated);
                }
            }
            sb.append(tmp.reverse());
            sb.append("]");
            sb.append(" ");
        }

        return sb.toString();
    }

    public byte[] toBytes() {
        int length = bitmap.length < 8 ? 1 : bitmap.length / 8;
        byte[] res = new byte[length];
        int ind = 0;

        for (int i = 0; i < bitmap.length; ) {
            byte b = 0;

            for (int j = 0; j < 8 && i < bitmap.length; j++, i++) {
                if (bitmap[i] == allocated) {
                    b = (byte) (b | (1 << j));
                }
            }
            res[ind++] = b;
        }
        return res;
    }

    public static BitMap fromBytes(byte[] bytes) {
        boolean[] res = new boolean[bytes.length * 8];
        int ind = 0;
        int amountAllocated = 0;

        for (int i = 0; i < bytes.length; i++) {
            byte bt = bytes[i];

            for (int j = 0; j < 8; j++) {

                int lastBit = bt & 1;

                amountAllocated += lastBit;

                res[ind++] = lastBit == 0 ? free : allocated;
                bt = (byte) (bt >> 1);
            }
        }

        return new BitMap(res, res.length - amountAllocated);
    }

    public Iterator<BitMapPair> iterator() {
        return new BitMapIterator(bitmap);
    }

    public int size() {
        return bitmap.length;
    }

    public boolean isFree(int ind) {
        return bitmap[ind] == free;
    }

    public class BitMapPair {
        public int ind;
        public int value;

        public BitMapPair(int ind, int value) {
            this.ind = ind;
            this.value = value;
        }
    }

    class BitMapIterator implements Iterator<BitMapPair> {

        private final boolean[] bitmap;
        int bit = 0;

        private BitMapIterator(boolean[] bitmap) {
            this.bitmap = bitmap;
        }

        @Override
        public boolean hasNext() {
            return bit < bitmap.length;
        }

        @Override
        public BitMapPair next() {
            BitMapPair res = new BitMapPair(bit, bitmap[bit] == free ? _free : _allocated);
            bit++;
            return res;
        }
    }
}
