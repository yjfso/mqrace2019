package io.openmessaging.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author yinjianfeng
 * @date 2019/8/31
 */
public class SimpleThreadLocal<T> {

    private Map<Thread, T> val = new HashMap<>();

    private Supplier<T> supplier;

    public static <T> SimpleThreadLocal<T> withInitial(Supplier<T> supplier) {
        return new SimpleThreadLocal<>(supplier);
    }

    private SimpleThreadLocal(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public T get() {
        Thread thread = Thread.currentThread();
        T t = val.get(thread);
        if (t == null) {
            synchronized (this) {
                t = supplier.get();
                val.put(thread, t);
            }
        }
        return t;
    }

    public Collection<T> getAll() {
        return val.values();
    }
}
