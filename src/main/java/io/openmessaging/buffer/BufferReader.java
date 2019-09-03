package io.openmessaging.buffer;

import io.openmessaging.common.Const;
import io.openmessaging.util.UnsafeHolder;

import java.nio.ByteBuffer;

import static io.openmessaging.buffer.ABuffer.BUFFER_OFFSET;
import static io.openmessaging.common.Const.MAX_GET_MSG_NUM;
import static io.openmessaging.util.UnsafeHolder.UNSAFE;

/**
 * @author yinjianfeng
 * @date 2019/9/1
 */
public class BufferReader {

    private ByteBuffer val;
//    private byte[] val;

    private boolean readBuffer;

    private long bufferOffset;

//    private int offset;

    private int size;

    private long address;

    public BufferReader(int bitSize) {
//        val = new byte[bitSize * MAX_GET_MSG_NUM];
        val = ByteBuffer.allocateDirect(bitSize * MAX_GET_MSG_NUM);
        address = UNSAFE.getLong(val, Const.BUFFER_ADDRESS_OFFSET);
    }

    public void initFromBuffer(long offset) {
        readBuffer = true;
        this.bufferOffset = offset - BUFFER_OFFSET;
    }

    public void init(int size) {
        readBuffer = false;
        this.bufferOffset = address;
        this.size = size;
        val.clear().limit(size);
    }

    public byte[] getBytes() {
//        return val;
        return null;
    }

    public ByteBuffer getByteBuffer() {
        return val;
    }

    public long getLong() {
        if (readBuffer) {
            try {
                return ABuffer.get(bufferOffset);
            } finally {
                bufferOffset += 8;
            }
        }
        try {
            return Long.reverseBytes(UnsafeHolder.UNSAFE.getLong(bufferOffset));
//            return val.getLong(offset);
        } finally {
            bufferOffset += 8;
        }
    }

    public int getSize() {
        return size;
    }

    public boolean isReadBuffer() {
        return readBuffer;
    }

    public long getAddress() {
        return address;
    }
}
