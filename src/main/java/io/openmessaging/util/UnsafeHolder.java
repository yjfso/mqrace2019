package io.openmessaging.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

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
}
