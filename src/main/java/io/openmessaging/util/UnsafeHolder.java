package io.openmessaging.util;

import io.openmessaging.common.Const;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * @author yinjianfeng
 * @date 2019/8/20
 */
public class UnsafeHolder {
    
    public final static Unsafe UNSAFE = getUnsafe();

    private static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static long bufferAddressOffset () {
        try{
            return UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void main(String[] args) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(100);
        for (int i = 0; i < 100; i++) {
            byteBuffer.put((byte)i);
        }

        byte[] result = new byte[100];

        UNSAFE.copyMemory(null, UNSAFE.getLong(byteBuffer, Const.BUFFER_ADDRESS_OFFSET),
                result, Const.ARRAY_BASE_OFFSET, 100);
        System.out.println(result);
    }
}
