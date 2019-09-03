package io.openmessaging.buffer;

import io.openmessaging.common.Const;
import io.openmessaging.util.UnsafeHolder;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.buffer.ABuffer.BUFFER_OFFSET;
import static io.openmessaging.common.Const.MAX_GET_MSG_NUM;
import static io.openmessaging.common.Const.T_SIZE;
import static io.openmessaging.util.UnsafeHolder.UNSAFE;

/**
 * @author yinjianfeng
 * @date 2019/8/26
 */
public class DirectBuffer {

    private int pos;

    final static int LENGTH = (Integer.MAX_VALUE - T_SIZE - MAX_GET_MSG_NUM * 42 * 15) >> 3;

    private ByteBuffer buffer = ByteBuffer.allocateDirect(LENGTH << 3);

    private long address = UNSAFE.getLong(buffer, Const.BUFFER_ADDRESS_OFFSET);

    public ByteBuffer require(int size) {
        pos += size;
        buffer.limit(pos);
        try {
            return buffer.slice();
        } finally {
            buffer.position(pos);
        }
    }

    public void write(FileChannel fileChannel, long length) {
        try {
            if (length <= 0) {
                return;
            }
            fileChannel.position(BUFFER_OFFSET + JvmBuffer.BYTE_LENGTH);
            buffer.limit((int)length);
            fileChannel.read(buffer);
//            byteBuffer.flip();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clear(){
        this.pos = 0;
        buffer.clear();
    }

    public long getLong(long pos) {
        return Long.reverseBytes(UnsafeHolder.UNSAFE.getLong(address + pos));
//        return ((LongBuffer)buffer).get(pos);
    }

}
