package io.openmessaging.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yinjianfeng
 * @date 2019/8/19
 */
public class DynamicIntArray {

    private final int[] buffer;

    private List<int[]> buffers;

    private int initSize;

    private byte sizeBit;

    private int pos;

    private int no = -1;

    private int realPos;

    public DynamicIntArray(int initSize, byte sizeBit) {
        this.initSize = initSize;
        this.sizeBit = sizeBit;
        buffer = new int[initSize];
    }

    public void put(int t) {
        int[] dst;
        if (no == -1) {
            dst = buffer;
        } else {
            dst = buffers.get(no);
        }
        dst[realPos++] = t;
        pos ++;
        if (realPos == dst.length) {
            realPos = 0;
            if (++no == 0) {
                buffers = new ArrayList<>();
            }
            buffers.add(new int[1 << sizeBit]);
        }

//        if (pos == initSize) {
//            buffers = new ArrayList<>();
//            buffers.add(new int[1 << sizeBit]);
//            buffers.get(0)[0] = t;
//        } else if (pos > initSize) {
//            int no = (pos - initSize) >>> sizeBit;
//            int rPos = (pos - initSize) - (no << sizeBit);
//            if (rPos == 0) {
//                buffers.add(new int[1 << sizeBit]);
//            }
//            buffers.get(no)[rPos] = t;
//        } else {
//            buffer[pos] = t;
//        }
//        pos ++;
    }

    public int get(int pos) {
        if (pos > initSize) {
            pos -= initSize;
            int no = pos >>> sizeBit;
            int rPos = pos - (no << sizeBit);
            return buffers.get(no + 1)[rPos];
        }
        return buffer[pos];
    }

    public int getPos() {
        return pos;
    }
}
