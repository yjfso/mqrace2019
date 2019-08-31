package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.common.Const;
import io.openmessaging.index.TIndex;
import io.openmessaging.util.ByteObjectPool;
import io.openmessaging.util.ByteUtil;

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

    private ThreadLocal<ByteObjectPool> bodyByte = ThreadLocal.withInitial(ByteObjectPool::new);

    public MsgReader(TIndex index){
        this.index = index;
    }

    public AtomicLong time = new AtomicLong();

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> messages = new LinkedList<>();
        ByteObjectPool byteObjectPool = bodyByte.get();
        IndexIterator indexIterator = index.getIterator(tMin, tMax);
        int length = indexIterator.getLength();
        if (length < 0) {
            System.out.println("=======");
        }
        long start = System.currentTimeMillis();
        byte[] as = atFile.read(indexIterator.getStartNo() << 3,  length << 3);
        byte[] bodies = bodyFile.read(indexIterator.getStartNo() * Const.BODY_SIZE,  length * Const.BODY_SIZE);
        long end = System.currentTimeMillis();
        time.getAndAdd(end - start);
        for (int i = 0; i < length; i++) {
            long t = indexIterator.nextT();
            if (t > tMax) {
                break;
            }
            long a = ByteUtil.bytes2long(as, i << 3);
            if (a != t) {
                System.out.println("===");
            }
            if (a < aMin || a > aMax) {
                continue;
            }

            byte[] body = byteObjectPool.borrowObject();
            System.arraycopy(bodies, i * Const.BODY_SIZE, body, 0, Const.BODY_SIZE);
            Message message = new Message(a, t, body);
            messages.add(message);
        }
        byteObjectPool.returnAll();
        if (((LinkedList<Message>) messages).getLast().getT() != Math.min(tMax, aMax)) {
            System.out.println("===");
        }
        return messages;
    }

    public void getMessageDone() {
        bodyFile = null;
        bodyByte = null;
        System.out.println("read time:" + time.get());
        time.set(0);
    }

    public long getAvg(long aMin, long aMax, long tMin, long tMax) {
        IndexIterator indexIterator = index.getIterator(tMin, tMax);
        int length = indexIterator.getLength();
        long start = System.currentTimeMillis();
        byte[] as = atFile.read(indexIterator.getStartNo() << 3,  length << 3);
        long end = System.currentTimeMillis();
        time.addAndGet(end - start);
        long ta = 0;
        int j = 0;
        for (int i = 0; i < length; i ++) {
            long t = indexIterator.nextT();
            if (t > tMax) {
                break;
            }
            long a = ByteUtil.bytes2long(as, i << 3);
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
