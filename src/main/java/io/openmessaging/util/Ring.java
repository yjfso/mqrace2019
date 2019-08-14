package io.openmessaging.util;

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

    public void add(E e) {
        if (es[writeIndex] != null) {
            while (true) {
                try {
//                    System.out.println("Ring full, wait...");
                    Thread.sleep(10);
                    if (es[writeIndex] == null) {
                        break;
                    }
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
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
        es[readIndex] = null;
        if (e != null) {
            readIndex ++;
            if (readIndex == es.length) {
                readIndex = 0;
            }
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

    public boolean isEmpty() {
        return readIndex == writeIndex && es[readIndex] == null;
    }

}
