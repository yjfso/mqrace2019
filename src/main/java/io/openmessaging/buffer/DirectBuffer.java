package io.openmessaging.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.buffer.ABuffer.BUFFER_OFFSET;

/**
 * @author yinjianfeng
 * @date 2019/8/26
 */
public class DirectBuffer {

    private int pos;

    final static int LENGTH = Integer.MAX_VALUE >> 3;

    private ByteBuffer byteBuffer = ByteBuffer.allocateDirect(LENGTH << 3);

    public ByteBuffer require(int size) {
        pos += size;
        byteBuffer.limit(pos);
        try {
            return byteBuffer.slice();
        } finally {
            byteBuffer.position(pos);
        }
    }

    public void write(FileChannel fileChannel, int length) {
        try {
            if (length <= 0) {
                return;
            }
            byteBuffer.clear();
            fileChannel.position(BUFFER_OFFSET + (JvmBuffer.LENGTH << 3));
            byteBuffer.limit(length);
            fileChannel.read(byteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clear(){
        this.pos = 0;
        byteBuffer.clear();
    }

    public long getLong(int pos) {
        return byteBuffer.getLong(pos << 3);
    }

}
