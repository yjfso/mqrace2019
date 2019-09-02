package io.openmessaging.buffer;

import io.openmessaging.util.ByteUtil;

import static io.openmessaging.buffer.ABuffer.BUFFER_OFFSET;
import static io.openmessaging.common.Const.MAX_GET_MSG_NUM;

/**
 * @author yinjianfeng
 * @date 2019/9/1
 */
public class BufferReader {

    private byte[] val;

    private boolean readBuffer;

    private int offset;

    private int size;

    public BufferReader(int bitSize) {
        val = new byte[bitSize * MAX_GET_MSG_NUM];
    }

    public void initFromBuffer(long offset) {
        readBuffer = true;
        this.offset = (int) ((offset - BUFFER_OFFSET) >> 3);
    }

    public void init(int size) {
        readBuffer = false;
        this.offset = 0;
        this.size = size;
    }

    public byte[] getBytes() {
        return val;
    }

    public long getLong() {
        if (readBuffer) {
            return ABuffer.get(offset ++);
        }
        try {
            return ByteUtil.bytes2long(val, offset);
        } finally {
            offset += 8;
        }
    }

    public int getSize() {
        return size;
    }

    public boolean isReadBuffer() {
        return readBuffer;
    }
}
