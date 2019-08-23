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

    private int size;

    private int pos;

    public DynamicIntArray(int initSize, int size) {
        this.initSize = initSize;
        this.size = size;
        buffer = new int[initSize];
    }

    public void put(int t) {
        if (pos == initSize) {
            buffers = new ArrayList<>();
            buffers.add(new int[size]);
            buffers.get(0)[0] = t;
        } else if (pos > initSize) {
            int no = (pos - initSize) / size;
            int rPos = (pos - initSize) % size;
            if (rPos == 0) {
                buffers.add(new int[size]);
            }
            buffers.get(no)[rPos] = t;
        } else {
            buffer[pos] = t;
        }
        pos ++;
    }

    public int get(int pos) {
//        if (pos >= this.pos) {
//            pos = this.pos - 1;
//        }
        if (pos > initSize) {
            int no = (pos - initSize) / size;
            int rPos = (pos - initSize) % size;
            return buffers.get(no)[rPos];
        }
        return buffer[pos];
    }

    public int getPos() {
        return pos;
    }
}
