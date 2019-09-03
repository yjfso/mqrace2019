package io.openmessaging.buffer;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.buffer.JvmBuffer.BYTE_LENGTH;

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

    private static boolean caching = true;

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

            System.out.println("======jvm cache a done 【" + cachingPos + "】 " + System.currentTimeMillis() + "=========");
            directBuffer.write(fileChannel, (int) (BUFFER_END - BUFFER_OFFSET - BYTE_LENGTH));
            System.out.println("======end cache a " + System.currentTimeMillis() + "=========");
            caching = false;
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static long get(long offset) {
        if (offset < BYTE_LENGTH) {
            return jvmBuffer.getLong(offset);
        } else {
            return directBuffer.getLong(offset - BYTE_LENGTH);
        }
    }

}
