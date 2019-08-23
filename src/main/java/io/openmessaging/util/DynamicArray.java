package io.openmessaging.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author yinjianfeng
 * @date 2019/8/18
 */
public class DynamicArray<T> {

    private final T[] buffer;

    private List<T[]> buffers;

    private Integer initSize;

    private Integer size;

    private Function<Integer, T[]> function;

    private int pos;

    public DynamicArray(Integer initSize, Integer size, Function<Integer, T[]> function) {
        this.initSize = initSize;
        this.size = size;
        this.function = function;
        buffer = function.apply(initSize);
    }

    public void put(T t) {
        if (pos == initSize) {
            buffers = new ArrayList<>();
            buffers.add(function.apply(size));
            buffers.get(0)[0] = t;
        } else if (pos > initSize) {
            int no = (pos - initSize) / size;
            int rPos = (pos - initSize) % size;
            if (rPos == 0) {
                buffers.add(function.apply(size));
            }
            buffers.get(no)[rPos] = t;
        } else {
            buffer[pos] = t;
        }
        pos ++;
    }

    public T get(int pos) {
        if (pos >= this.pos) {
            pos = this.pos - 1;
        }
        if (pos > initSize) {
            int no = (pos - initSize) / size;
            int rPos = (pos - initSize) % size;
            return buffers.get(no)[rPos];
        }
        return buffer[pos];
    }
}