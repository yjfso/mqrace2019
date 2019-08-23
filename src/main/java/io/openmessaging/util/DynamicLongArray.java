package io.openmessaging.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yinjianfeng
 * @date 2019/8/19
 */
public class DynamicLongArray {

    private final long[] buffer;

    private List<long[]> buffers;

    private Integer initSize;

    private Integer size;

    private int pos;

    DynamicLongArray(Integer initSize, Integer size) {
        this.initSize = initSize;
        this.size = size;
        buffer = new long[initSize];
    }

    public void put(long t) {
        if (pos == initSize) {
            buffers = new ArrayList<>();
            buffers.add(new long[size]);
            buffers.get(0)[0] = t;
        } else if (pos >= size) {
            int no = (pos - initSize) / size;
            int rPos = (pos - initSize) % size;
            if (rPos == 0) {
                buffers.add(new long[size]);
            }
            buffers.get(no)[rPos] = t;
        } else {
            buffer[pos] = t;
        }
        pos ++;
    }

    public void clear() {
        buffers = null;
        pos = 0;
    }

    public boolean isEmpty() {
        return pos == 0;
    }

}
