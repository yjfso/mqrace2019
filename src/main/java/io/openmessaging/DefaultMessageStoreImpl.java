package io.openmessaging;

import io.openmessaging.common.BoolLock;
import io.openmessaging.index.TIndex;
import io.openmessaging.store.MsgReader;
import io.openmessaging.store.MsgReaderInitiator;
import io.openmessaging.store.MsgWriter;
import io.openmessaging.store.MsgWriterInitiator;

import java.util.List;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private BoolLock readAvgInit = new BoolLock();

    private TIndex index = new TIndex();

    private MsgWriter msgWriter = new MsgWriterInitiator(index, this);

    private MsgReader msgReader = new MsgReaderInitiator(((MsgWriterInitiator)msgWriter).getMsgWriter(), index, this);

    @Override
    public void put(Message message) {
        msgWriter.put(message);
    }

    public void setMsgWriter(MsgWriter msgWriter) {
        this.msgWriter = msgWriter;
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        return msgReader.getMessage(aMin, aMax, tMin, tMax);
    }

    public void setMsgReader(MsgReader msgReader) {
        msgWriter = null;
        this.msgReader = msgReader;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (readAvgInit.tryLock()) {
            msgReader.getMessageDone();
        }
        return msgReader.getAvg(aMin, aMax, tMin, tMax);
    }

}
