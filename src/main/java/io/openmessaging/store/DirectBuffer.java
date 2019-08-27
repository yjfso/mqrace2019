package io.openmessaging.store;

import java.nio.ByteBuffer;

/**
 * @author yinjianfeng
 * @date 2019/8/26
 */
public class DirectBuffer {

    private final static int _1G = 1024 * 1024 * 1024;

    private static int pos;

    private static ByteBuffer byteBuffer = ByteBuffer.allocateDirect(_1G);

    public static ByteBuffer ask(int size) {
        pos += size;
        byteBuffer.limit(pos);
        try {
            return byteBuffer.slice();
        } finally {
            byteBuffer.position(pos);
        }
    }

    public static void returnAll() {
        pos = 0;
    }

}
