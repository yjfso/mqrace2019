package io.openmessaging.buffer;

import io.openmessaging.util.UnsafeHolder;

import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.buffer.ABuffer.*;
import static io.openmessaging.common.Const.LONG_ARRAY_BASE_OFFSET;

/**
 * @author yinjianfeng
 * @date 2019/8/31
 */
public class JvmBuffer {

    final static int LENGTH = (int) (((500L << 20) + Integer.MAX_VALUE) >> 3);

    final static long BYTE_LENGTH = (long)LENGTH << 3;

    private final long[] val = new long[LENGTH];

    private long[] secondVal;

    public void write(FileChannel fileChannel) {
        try {
            long endBuffer = Math.min(BUFFER_OFFSET + BYTE_LENGTH, BUFFER_END);
            int readTime = (int)((endBuffer - BUFFER_OFFSET) >> 30);

            long startByteOffset = BUFFER_OFFSET;

            for (int i = 0; i <= readTime; i++) {
                long byteLength;
                if (i == readTime) {
                    byteLength = endBuffer - startByteOffset;
                } else {
                    byteLength = 1 << 30;
                }

                LongBuffer longBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startByteOffset, byteLength)
                        .asLongBuffer();
                longBuffer.get(val, (i << 27), (int)byteLength >>> 3);
                startByteOffset += byteLength;
                cachingPos = startByteOffset;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getMessageDone(FileChannel fileChannel) {
        System.out.println("==============second JVM Buffer start=============");
        long byteLength = Math.min(BUFFER_OFFSET, 300 << 20);
        int length = (int) (byteLength >> 3);
        secondVal = new long[length];
        try {
            LongBuffer longBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, Math.max(0, BUFFER_OFFSET - 300 << 20), byteLength)
                    .asLongBuffer();
            longBuffer.get(secondVal);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BUFFER_OFFSET -= length;
        System.out.println("==============second JVM Buffer end=============");
    }

    public long getLong(long pos) {
        return UnsafeHolder.UNSAFE.getLong(val, LONG_ARRAY_BASE_OFFSET + pos);
//        return val[pos];
    }
}
