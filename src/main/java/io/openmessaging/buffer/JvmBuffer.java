package io.openmessaging.buffer;

import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.buffer.Buffer.BUFFER_OFFSET;
import static io.openmessaging.buffer.Buffer.LONG_NUM_IN_1G;

/**
 * @author yinjianfeng
 * @date 2019/8/31
 */
public class JvmBuffer {

    private final long[] val = new long[Integer.MAX_VALUE >>> 8];

    public void write(FileChannel fileChannel) {
        try {
            LongBuffer longBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, BUFFER_OFFSET, 1 << 30)
                    .asLongBuffer();
            longBuffer.get(val, 0, LONG_NUM_IN_1G);
            longBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, BUFFER_OFFSET + 1 << 30, 1 << 30).asLongBuffer();
            longBuffer.get(val, LONG_NUM_IN_1G, LONG_NUM_IN_1G);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getLong(int pos) {
        return val[pos];
    }
}
