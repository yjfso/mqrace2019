package io.openmessaging.buffer;

import io.openmessaging.common.Const;
import io.openmessaging.store.Vfs;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;

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

    private static CountDownLatch latch = new CountDownLatch(1);

    static long BUFFER_OFFSET;

    static long BUFFER_END;

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
            System.out.println("======start cache a " + System.currentTimeMillis() + "=========");
            jvmBuffer = new JvmBuffer();
            jvmBuffer.write(fileChannel);

            directBuffer.write(fileChannel, (int) (BUFFER_END - BUFFER_OFFSET - (JvmBuffer.LENGTH << 3)));
            System.out.println("======end cache a " + System.currentTimeMillis() + "=========");
            caching = false;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        latch.countDown();
    }

    public static long get(int offset) {
        if (offset < JvmBuffer.LENGTH) {
            return jvmBuffer.getLong(offset);
        } else {
            return directBuffer.getLong(offset - JvmBuffer.LENGTH);
        }
    }

    public static void main(String[] args) {
        String fileName = Const.DATA_PATH + Vfs.VfsEnum.at.name();
        try {
            FileChannel fileChannel = FileChannel.open(Paths.get(fileName), StandardOpenOption.READ, StandardOpenOption.WRITE);
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1 << 30);
            for (int i = 0; i < 1024 * 1024 * 1024; i++) {
                if (!byteBuffer.hasRemaining()) {
                    byteBuffer.flip();
                    fileChannel.write(byteBuffer);
                    byteBuffer.clear();
                }
                byteBuffer.putLong(i);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        writeDone(1024*1024*1024);
        Vfs.VfsEnum.at.vfs.cache();
    }

}
