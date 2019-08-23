package io.openmessaging.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yinjianfeng
 * @date 2019/8/19
 */
public class DynamicByteArray {

    private final byte[] buffer;

    private List<byte[]> buffers;

    private Integer initSize;

    private Integer size;

    private int pos;

    public DynamicByteArray(Integer initSize, Integer size) {
        this.initSize = initSize;
        this.size = size;
        buffer = new byte[initSize];
    }

    public void put(byte t) {
        if (pos == initSize) {
            buffers = new ArrayList<>();
            buffers.add(new byte[size]);
            buffers.get(0)[0] = t;
        } else if (pos >= initSize) {
            int no = (pos - initSize) / size;
            int rPos = (pos - initSize) % size;
            if (rPos == 0) {
                buffers.add(new byte[size]);
            }
            buffers.get(no)[rPos] = t;
        } else {
            buffer[pos] = t;
        }
        pos ++;
    }

    public byte[] dump() {
        byte[] result = new byte[pos];
        if (pos <= initSize) {
            System.arraycopy(buffer, 0, result, 0, pos);
        } else {
            System.arraycopy(buffer, 0, result, 0, initSize);
            int destPos = initSize;
            for (int i = 0; i < buffers.size(); i++) {
                byte[] bytes = buffers.get(i);
                System.arraycopy(bytes, 0, result, destPos, Math.min(pos - destPos, size));
                destPos += size;
            }
        }
        return result;
    }

    public void clear() {
        pos = 0;
    }

    public boolean isEmpty() {
        return pos == 0;
    }
}
