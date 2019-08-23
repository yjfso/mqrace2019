package io.openmessaging.index;

import io.openmessaging.util.DynamicArray;
import io.openmessaging.util.DynamicByteArray;
import io.openmessaging.util.DynamicIntArray;

import static io.openmessaging.common.Const.T_INTERVAL_BIT;

/**
 * @author yinjianfeng
 * @date 2019/8/16
 */
public class TIndex {

    public final DynamicArray<byte[]> segments = new DynamicArray<>(4000000, 1000, byte[][]::new);

    public final DynamicIntArray pileIndexes = new DynamicIntArray(4000000, 1000);

    private DynamicByteArray tmpByteArray = new DynamicByteArray(512, 10);

    public long startPile;

    private long lastPile;

    private int pos = 0;

    public void put(long num) {
        long pile = num >> T_INTERVAL_BIT;
        if (pos == 0) {
            startPile = pile;
            lastPile = pile;
            pileIndexes.put(pos);
        }

        if (pile != lastPile) {
            for (int i = (int)(pile - lastPile - 1); i > 0; i--) {
                segments.put(null);
                pileIndexes.put(pos);
            }
            if (!tmpByteArray.isEmpty()) {
                byte[] bytes = tmpByteArray.dump();
//                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
//                byteBuffer.put(bytes);
                segments.put(bytes);
                tmpByteArray.clear();
            }
            pileIndexes.put(pos);
            lastPile = pile;
        }
        pos ++;
        tmpByteArray.put((byte)(num - (pile << T_INTERVAL_BIT)));
    }

    public void writeDone() {
        byte[] bytes = tmpByteArray.dump();
        segments.put(bytes);
        tmpByteArray = null;
    }

}
