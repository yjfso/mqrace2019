package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.buffer.BufferReader;
import io.openmessaging.common.Const;
import io.openmessaging.index.TIndex;
import io.openmessaging.util.DynamicArray;
import io.openmessaging.util.SimpleThreadLocal;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

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

    public MsgReader(TIndex index){
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

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> messages = new LinkedList<>();
        DynamicArray<Message> byteObjectPool = bodyByte.get();
        IndexIterator indexIterator = index.getIterator(tMin, tMax);
        int length = indexIterator.getLength();

        BufferReader as = atFile.read(indexIterator.getStartNo() << 3,  length << 3);
        BufferReader bodies = bodyFile.read(indexIterator.getStartNo() * Const.BODY_SIZE,  length * Const.BODY_SIZE);

        ByteBuffer byteBuffer = bodies.getByteBuffer();
//        BufferReader as = asFuture.get();
//        BufferReader bodies = bodiesFuture.get();
        System.out.println("read aMin:" + aMin + " aMax:" + aMax + " tMin:" + tMin + " tMax:" + tMax +
                " isReadBuffer:" + as.isReadBuffer() + " bodies:" + byteBuffer.toString());

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
            byteBuffer.get(message.getBody());
//            System.arraycopy(bodies.getBytes(), i * Const.BODY_SIZE, message.getBody(), 0, Const.BODY_SIZE);
            message.setA(a);
            message.setT(t);
            messages.add(message);
            if (ByteBuffer.wrap(message.getBody()).getLong(26) != a) {
                System.out.println("====");
            }
        }
        byteObjectPool.reset();
        return messages;
    }

    public void getMessageDone() {
        Vfs.VfsEnum.getMsgDone();
        bodyByte = null;
    }

    public long getAvg(long aMin, long aMax, long tMin, long tMax) {
        IndexIterator indexIterator = index.getIterator(tMin, tMax);
        int length = indexIterator.getLength();
        BufferReader as = atFile.read(indexIterator.getStartNo() << 3,  length << 3);//.get();
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
