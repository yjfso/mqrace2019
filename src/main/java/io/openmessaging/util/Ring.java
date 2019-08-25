package io.openmessaging.util;

import java.util.function.Supplier;

/**
 * @author yinjianfeng
 * @date 2019/8/14
 */
public class Ring<E> {

    private E[] es;

    private int readIndex;

    private int writeIndex;

    public Ring(E[] es) {
        this.es = es;
    }

    public Ring<E> fill (Supplier<E> supplier) {
        for (int i = 0; i < es.length; i++) {
            es[i] = supplier.get();
        }
        return this;
    }

    public void add(E e) {
        while (es[writeIndex] != null) {
            try {
                Thread.sleep(3);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        es[writeIndex++] = e;
        if (writeIndex == es.length) {
            writeIndex = 0;
        }
    }
    public void add1(E e) {
        while (es[writeIndex] != null) {
            try {
                Thread.sleep(3);
                System.out.println("return" + writeIndex + "|" + readIndex);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        es[writeIndex++] = e;
        if (writeIndex == es.length) {
            writeIndex = 0;
        }
    }

    public boolean isFull() {
        return readIndex == writeIndex && es[readIndex] != null;
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

    public E getLast() {
        int last = writeIndex;
        if (last == 0) {
            return es[es.length - 1];
        }
        return es[last - 1];
    }

    public int getReadIndex() {
        return readIndex;
    }

    public boolean isEmpty() {
        return readIndex == writeIndex && es[readIndex] == null;
    }

    @Override
    public String toString() {
        return "readIndex" + readIndex + ";writeIndex:" + writeIndex;
    }
}
