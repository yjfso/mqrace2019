package io.openmessaging.store;

import io.openmessaging.buffer.BufferReader;

import java.util.concurrent.CountDownLatch;

/**
 * @author yinjianfeng
 * @date 2019/9/1
 */
public class VfsFuture {

    private CountDownLatch latch;

    private BufferReader bufferReader;

    public VfsFuture(int bitSize) {
        bufferReader = new BufferReader(bitSize);
    }

    public BufferReader get() {
        try {
            if (latch != null) {
                latch.await();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return bufferReader;
    }

    public void done() {
        latch.countDown();
    }

    public BufferReader forceGet() {
        return bufferReader;
    }

    public void init(int size) {
        latch = new CountDownLatch(1);
        bufferReader.init(size);
    }
}
