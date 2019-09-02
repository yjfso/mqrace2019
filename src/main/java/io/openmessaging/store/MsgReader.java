package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.buffer.Buffer;
import io.openmessaging.buffer.BufferReader;
import io.openmessaging.common.Const;
import io.openmessaging.index.TIndex;
import io.openmessaging.util.DynamicArray;
import io.openmessaging.util.SimpleThreadLocal;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yinjianfeng
 * @date 2019/8/5
 */
public class MsgReader {

    private Vfs.VfsEnum atFile = Vfs.VfsEnum.at;

    private Vfs.VfsEnum bodyFile = Vfs.VfsEnum.body;

    private TIndex index;

    private SimpleThreadLocal<DynamicArray<Message>> bodyByte;

    MsgReader() {}

    MsgReader(TIndex index){
        this.index = index;
        bodyByte = SimpleThreadLocal.withInitial(
                () -> new DynamicArray<>(25_0000, 10000, size -> {
                    Message[] messages = new Message[size];
                    for (int i = 0; i < size; i++) {
                        messages[i] = new Message();
                        messages[i].setBody(new byte[Const.BODY_SIZE]);
                    }
                    return messages;
                })
        );
    }

    public AtomicLong time = new AtomicLong();

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> messages = new LinkedList<>();
        DynamicArray<Message> byteObjectPool = bodyByte.get();
        Vfs.VfsEnum.at.vfs.cache();
//        IndexIterator indexIterator = index.getIterator(tMin, tMax);
        IndexIterator indexIterator = new IndexIterator(new TBits());
        indexIterator.initBase(1, (int)((3L * Integer.MAX_VALUE) >>> 3));
        indexIterator.setEndNo((int)(1000 + (3L * Integer.MAX_VALUE) >>> 3));
        int length = indexIterator.getLength();
        long start = System.currentTimeMillis();

        VfsFuture asFuture = atFile.read(indexIterator.getStartNo() << 3,  length << 3);
        VfsFuture bodiesFuture = bodyFile.read(indexIterator.getStartNo() * Const.BODY_SIZE,  length * Const.BODY_SIZE);

        BufferReader as = asFuture.get();
        BufferReader bodies = bodiesFuture.get();

        long end = System.currentTimeMillis();
        time.getAndAdd(end - start);
        for (int i = 0; i < length; i++) {
            long t = indexIterator.nextT();
            if (t > tMax) {
                break;
            }

            long a = as.getLong();
            if (a < aMin || a > aMax) {
                continue;
            }
            Message message = byteObjectPool.get();
            System.arraycopy(bodies.getBytes(), i * Const.BODY_SIZE, message.getBody(), 0, Const.BODY_SIZE);
            message.setA(a);
            message.setT(t);
            messages.add(message);
        }
        byteObjectPool.reset();
        return messages;
    }

    public void getMessageDone() {
        bodyFile.close();
        bodyByte = null;
        System.out.println("read time:" + time.get());
        time.set(0);
    }

    public long getAvg(long aMin, long aMax, long tMin, long tMax) {
        IndexIterator indexIterator = index.getIterator(tMin, tMax);
        int length = indexIterator.getLength();
        long start = System.currentTimeMillis();
        BufferReader as = atFile.read(indexIterator.getStartNo() << 3,  length << 3).get();
        long end = System.currentTimeMillis();
        time.addAndGet(end - start);
        long ta = 0;
        int j = 0;
        for (int i = 0; i < length; i ++) {
            long t = indexIterator.nextT();
            if (t > tMax) {
                break;
            }
            long a = as.getLong();
            if (a < aMin || a > aMax) {
                continue;
            }
            ta += a;
            j++;
        }
        if (j == 0) {
            return 0;
        }
        return ta/j;
    }

}
