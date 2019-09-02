package io.openmessaging.buffer;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;

/**
 * @author yinjianfeng
 * @date 2019/9/1
 */
public class Buffer {

    public enum InBuffer {
        //
        in,
        half,
        none
    }

    private static DirectBuffer directBuffer = new DirectBuffer();

    private static JvmBuffer jvmBuffer;

    private static boolean caching = true;

    private static CountDownLatch latch = new CountDownLatch(1);

    final static int LONG_NUM_IN_1G = 1 << 27;

    final static int JVM_END = 1 << 28;

    public final static long BUFFER_OFFSET = 3L * Integer.MAX_VALUE;

    public final static long BUFFER_END = 5L * Integer.MAX_VALUE;

    public static ByteBuffer requireDirect(int size) {
        return directBuffer.require(size);
    }

    public static void writeDone() {
        directBuffer.clear();
        jvmBuffer = new JvmBuffer();
    }

    public static InBuffer inBuffer(long offset, int size) {
        if (caching) {
            if (offset < BUFFER_OFFSET || (offset + size) > BUFFER_END) {
                return InBuffer.none;
            }
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long endOffset = offset + size;
        if (offset >= BUFFER_OFFSET && endOffset <= BUFFER_END) {
            return InBuffer.in;
        } else {
            return InBuffer.none;
        }
    }

    public static void cacheA(FileChannel fileChannel) {
        try {
            jvmBuffer.write(fileChannel);
            directBuffer.write(fileChannel);
            caching = false;
        } catch (Exception e) {
            e.printStackTrace();
        }
        latch.countDown();
    }

    public static long get(int offset) {
        if (offset < JVM_END) {
            return jvmBuffer.getLong(offset);
        } else {
            return directBuffer.getLong(offset - JVM_END);
        }
    }
}
