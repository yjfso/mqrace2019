package io.openmessaging;

import io.openmessaging.bean.ThreadMessage;
import io.openmessaging.bean.ThreadMessageManager;
import io.openmessaging.common.BoolLock;
import io.openmessaging.index.TIndex;
import io.openmessaging.store.MsgReader;
import io.openmessaging.store.MsgWriter;

import java.util.List;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private BoolLock putInit = new BoolLock();

    private BoolLock readInit = new BoolLock();

    private BoolLock readAvgInit = new BoolLock();

    private ThreadMessageManager threadMessageManager = new ThreadMessageManager();

    private ThreadLocal<ThreadMessage> messages = ThreadLocal.withInitial(
            () -> new ThreadMessage(threadMessageManager)
    );

    private TIndex index = new TIndex();

    private MsgWriter msgWriter = new MsgWriter(index, threadMessageManager);

    private volatile MsgReader msgReader;

    @Override
    public void put(Message message) {
        messages.get().put(message);
        if (putInit.tryLock()) {
            msgWriter.start();
        }
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (readInit.tryLock()) {
            msgWriter.stop();
            msgReader = new MsgReader(index);
            System.out.println("======reader is ready=======");
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
        if (readAvgInit.tryLock()) {
            msgReader.getMessageDone();
        }
        return msgReader.getAvg(aMin, aMax, tMin, tMax);
    }

}
