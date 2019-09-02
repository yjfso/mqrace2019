package io.openmessaging.index;

import io.openmessaging.store.IndexIterator;
import io.openmessaging.store.TBits;
import io.openmessaging.util.DynamicIntArray;
import io.openmessaging.util.SimpleThreadLocal;

import static io.openmessaging.common.Const.T_INTERVAL_BIT;

/**
 * @author yinjianfeng
 * @date 2019/8/16
 */
public class TIndex {

    //存储每段数据头部的No值
    public final DynamicIntArray pileIndexes = new DynamicIntArray(16_000_000, (byte) 10);

    //存储每段Tbits头部的index值
    public final DynamicIntArray pileSegment = new DynamicIntArray(16_000_000, (byte) 10);

    private TBits tBits = new TBits();

    private final SimpleThreadLocal<IndexIterator> indexIteratorLocal = SimpleThreadLocal.withInitial(() -> new IndexIterator(tBits));

    public long startPile;

    //lastT when write done
    private long lastPile;

    private int lastSurplus;

    private int equalNum;

    private int no;

    public void put(long t) {
        long pile = t >> T_INTERVAL_BIT;
        if (no == 0) {
            startPile = pile;
            lastPile = pile;
            pileIndexes.put(0);
            pileSegment.put(0);
            System.out.println("put first t:" + t + " at " + System.currentTimeMillis());
        }
        //0~255
        int surplus = (int) (t - (pile << T_INTERVAL_BIT));
        if (surplus != lastSurplus) {
            if (equalNum > 0) {
                tBits.setBit(equalNum);
                equalNum = 0;
            }
            tBits.clearBit(surplus - lastSurplus);
            lastSurplus = surplus;
        }
        equalNum ++;
        if (pile != lastPile) {
            int s = tBits.getPos();
            for (int i = (int)(pile - lastPile); i > 0; i--) {
                pileIndexes.put(no);
                pileSegment.put(s);
            }
            lastPile = pile;
        }
        no ++;
    }

    public void writeDone() {
        if (equalNum > 0) {
            tBits.setBit(equalNum);
        }
        lastPile = (lastPile << T_INTERVAL_BIT) + lastSurplus;
        tBits.writeDone();
    }

    public IndexIterator getIterator(long tMin, long tMax) {
        IndexIterator indexIterator = indexIteratorLocal.get();

        {
            long pile = (tMin >> T_INTERVAL_BIT);
            int realPile = (int)(pile - startPile);
            long baseT;
            if (realPile < 0) {
                realPile = 0;
                baseT = startPile << T_INTERVAL_BIT;
            } else {
                baseT = pile << T_INTERVAL_BIT;
            }

            indexIterator.initBase(baseT, pileIndexes.get(realPile));
            tBits.initIteratorStart(pileSegment.get(realPile), (int)(tMin - baseT), indexIterator);
        }


        if (tMax > lastPile) {
            indexIterator.setEndNo(no);
        } else {
            long pile = (tMax >> T_INTERVAL_BIT);
            int realPile = (int)(pile - startPile);
            long endNo = tBits.endNo(pileSegment.get(realPile), (int)(tMax - (pile << T_INTERVAL_BIT)));
            indexIterator.setEndNo(pileIndexes.get(realPile) + endNo);
        }

        return indexIterator;
    }

    public int getNo() {
        return no;
    }

    public long getLastT() {
        return lastPile;
    }
}
