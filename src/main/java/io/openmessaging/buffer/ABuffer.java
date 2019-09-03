package io.openmessaging.buffer;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author yinjianfeng
 * @date 2019/9/1
 */
public class ABuffer {

    public enum InBuffer {
        //
        in,
        half,
        none
    }

    private static DirectBuffer directBuffer = new DirectBuffer();

    private static JvmBuffer jvmBuffer;

    private static volatile boolean caching = true;

    static long cachingPos;

    static volatile long BUFFER_OFFSET;

    static long BUFFER_END;

    private static FileChannel fileChannel;

    public static ByteBuffer requireDirect(int size) {
        return directBuffer.require(size);
    }

    public static void writeDone(int no) {
        directBuffer.clear();

        //计算缓存位置
        long availableLength = DirectBuffer.LENGTH + JvmBuffer.LENGTH;
        if (availableLength >= no) {
            BUFFER_OFFSET = 0;
            BUFFER_END = no;
        } else {
            BUFFER_OFFSET = (no - availableLength) >> 1;
            BUFFER_END = availableLength + BUFFER_OFFSET;
        }
        BUFFER_OFFSET <<= 3;
        BUFFER_END <<= 3;
        cachingPos = BUFFER_OFFSET;
    }

    public static void getMessageDone() {
//        jvmBuffer.getMessageDone(fileChannel);
    }

    public static InBuffer inBuffer(long offset, int size) {
        long endOffset = offset + size;
        if (offset >= BUFFER_OFFSET && endOffset <= BUFFER_END) {
            if (caching && endOffset > cachingPos) {
                return InBuffer.none;
            }
            return InBuffer.in;
        } else {
            return InBuffer.none;
        }
    }

    public static void cacheA(FileChannel fileChannel) {
        ABuffer.fileChannel = fileChannel;
        try {
            System.out.println("======start cache a " + System.currentTimeMillis() + "=========");
            jvmBuffer = new JvmBuffer();
            jvmBuffer.write(fileChannel);

            directBuffer.write(fileChannel, (int) (BUFFER_END - BUFFER_OFFSET - (JvmBuffer.LENGTH << 3)));
            System.out.println("======end cache a " + System.currentTimeMillis() + "=========");
            caching = false;
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static long get(int offset) {
        if (offset < JvmBuffer.LENGTH) {
            return jvmBuffer.getLong(offset);
        } else {
            return directBuffer.getLong(offset - JvmBuffer.LENGTH);
        }
    }

}
