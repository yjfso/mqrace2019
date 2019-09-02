package io.openmessaging.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author yinjianfeng
 * @date 2019/8/26
 */
public class DirectBuffer {

    private int pos;

    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Integer.MAX_VALUE);

    public ByteBuffer require(int size) {
        pos += size;
        byteBuffer.limit(pos);
        try {
            return byteBuffer.slice();
        } finally {
            byteBuffer.position(pos);
        }
    }

    public void write(FileChannel fileChannel) {
        try {
            byteBuffer.clear();
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
