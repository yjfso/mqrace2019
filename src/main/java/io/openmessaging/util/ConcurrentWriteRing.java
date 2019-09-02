package io.openmessaging.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * @author yinjianfeng
 * @date 2019/8/14
 */
public class ConcurrentWriteRing<E> {

    private E[] es;

    private int readIndex;

//    private int writeIndex;

    private AtomicInteger writeIndex = new AtomicInteger();

    public ConcurrentWriteRing(E[] es) {
        this.es = es;
    }

    public ConcurrentWriteRing<E> fill (Supplier<E> supplier) {
        for (int i = 0; i < es.length; i++) {
            es[i] = supplier.get();
        }
        return this;
    }

    public void threadSafeAdd(E e) {
        int writeIndex = this.writeIndex.getAndIncrement();
        if (writeIndex >= es.length) {
            synchronized (this) {
                writeIndex = this.writeIndex.getAndIncrement();
                if (writeIndex >= es.length) {
                    writeIndex = 0;
                    this.writeIndex.set(1);
                }
            }
        }
        while (es[writeIndex] != null) {
            try {
                Thread.sleep(3);
                System.out.println("return" + writeIndex + "|" + readIndex);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        es[writeIndex] = e;
    }

    public boolean isFull() {
        return readIndex == writeIndex.get() && es[readIndex] != null;
    }

    public E pop() {
        final E e = es[readIndex];
        if (e != null) {
            es[readIndex] = null;
            if (++readIndex == es.length) {
                readIndex = 0;
            }
        }
        return e;
    }

    public E popWait() {
        E e = es[readIndex];
        while (e == null) {
            try {
                System.out.println("pop wait...");
                Thread.sleep(1);
                e = es[readIndex];
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        es[readIndex] = null;
        if (++readIndex == es.length) {
            readIndex = 0;
        }
        return e;
    }

    public int getReadIndex() {
        return readIndex;
    }

    @Override
    public String toString() {
        return "readIndex" + readIndex + ";writeIndex:" + writeIndex;
    }
}
