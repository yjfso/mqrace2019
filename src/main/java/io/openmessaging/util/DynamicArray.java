package io.openmessaging.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author yinjianfeng
 * @date 2019/8/18
 */
public class DynamicArray<T> {

    private T[] buffer;

    private List<T[]> buffers;

    private Integer initSize;

    private Integer size;

    private Function<Integer, T[]> function;

    private int no;

    private int realPos;

    public DynamicArray(Integer initSize, Integer size, Function<Integer, T[]> function) {
        this.initSize = initSize;
        this.size = size;
        this.function = function;
        buffer = function.apply(initSize);
        buffers = new ArrayList<>();
        buffers.add(buffer);
    }

    public T get() {
        if (realPos == buffer.length) {
            if (++no == buffers.size()) {
                buffers.add(function.apply(size));
            }
            buffer = buffers.get(no);
        }
        return buffer[realPos++];
    }

    public void reset() {
        no = 0;
        realPos = 0;
        buffer = buffers.get(0);
    }
}