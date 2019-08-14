package io.openmessaging;

import io.openmessaging.bean.ThreadMessage;
import io.openmessaging.common.BoolLock;
import io.openmessaging.index.DichotomicIndex;
import io.openmessaging.store.MsgReader;
import io.openmessaging.store.MsgWriter;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private BoolLock putInit = new BoolLock();

    private BoolLock readInit = new BoolLock();

    private NavigableMap<Long, List<Message>> msgMap = new TreeMap<Long, List<Message>>();

    private ThreadLocal<ThreadMessage> messages = ThreadLocal.withInitial(ThreadMessage::new);

    private DichotomicIndex index = new DichotomicIndex();

    private MsgWriter msgWriter = new MsgWriter(index);

    private volatile MsgReader msgReader;

    private long minA = Integer.MAX_VALUE;

    private long minT = Integer.MAX_VALUE;

    private long maxA = 0;

    private long maxT = 0;

    @Override
    public void put(Message message) {
        minA = Math.min(message.getA(), minA);
        maxA = Math.max(message.getA(), maxA);
        minT = Math.min(message.getT(), minT);
        maxT = Math.max(message.getT(), maxT);

        messages.get().put(message);
        if (putInit.tryLock()) {
            msgWriter.start();
        }
    }


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (readInit.tryLock()) {
            msgWriter.stop();
            System.out.println(minA + "," + maxA + "," + minT + "," + maxT);
            msgReader = new MsgReader(index);
            msgWriter = null;
        }
        while (msgReader == null) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return msgReader.getMessage(aMin, aMax, tMin, tMax);
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return msgReader.getAvg(aMin, aMax, tMin, tMax);
    }

}
