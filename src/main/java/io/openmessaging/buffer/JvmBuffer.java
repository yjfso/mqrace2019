package io.openmessaging.buffer;

import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.buffer.ABuffer.BUFFER_END;
import static io.openmessaging.buffer.ABuffer.BUFFER_OFFSET;

/**
 * @author yinjianfeng
 * @date 2019/8/31
 */
public class JvmBuffer {

    final static int LENGTH = Integer.MAX_VALUE >> 3;

    private final long[] val = new long[LENGTH];

    public void write(FileChannel fileChannel) {
        try {
            long endBuffer = Math.min(BUFFER_OFFSET + (((long) LENGTH) << 3), BUFFER_END);
            int readTime = (int)((endBuffer - BUFFER_OFFSET) >> 30);

            long startByteOffset = BUFFER_OFFSET;

            for (int i = 0; i <= readTime; i++) {
                long byteLength = Math.min(endBuffer - startByteOffset, 1 << 30);

                LongBuffer longBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startByteOffset, byteLength)
                        .asLongBuffer();
                longBuffer.get(val, (i << 27), (int)byteLength >>> 3);
                startByteOffset += (1 << 30);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getLong(int pos) {
        return val[pos];
    }
}
