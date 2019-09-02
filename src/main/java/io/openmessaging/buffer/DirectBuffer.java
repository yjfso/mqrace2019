package io.openmessaging.buffer;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.buffer.ABuffer.BUFFER_OFFSET;

/**
 * @author yinjianfeng
 * @date 2019/8/26
 */
public class DirectBuffer {

    private int pos;

    final static int LENGTH = Integer.MAX_VALUE >> 3;

    private Buffer buffer = ByteBuffer.allocateDirect(LENGTH << 3);

    public ByteBuffer require(int size) {
        pos += size;
        buffer.limit(pos);
        try {
            return ((ByteBuffer) buffer).slice();
        } finally {
            buffer.position(pos);
        }
    }

    public void write(FileChannel fileChannel, int length) {
        try {
            if (length <= 0) {
                return;
            }
            ByteBuffer byteBuffer = (ByteBuffer) buffer;
            fileChannel.position(BUFFER_OFFSET + (JvmBuffer.LENGTH << 3));
            byteBuffer.limit(length);
            fileChannel.read(byteBuffer);
            byteBuffer.flip();
            buffer = byteBuffer.asLongBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clear(){
        this.pos = 0;
        buffer.clear();
    }

    public long getLong(int pos) {
        return ((LongBuffer)buffer).get(pos);
    }

}
