package io.openmessaging.store;

import io.openmessaging.DefaultMessageStoreImpl;
import io.openmessaging.Message;
import io.openmessaging.common.BoolLock;
import io.openmessaging.index.TIndex;

import java.util.List;

/**
 * @author yinjianfeng
 * @date 2019/9/1
 */
public class MsgReaderInitiator extends MsgReader {

    private BoolLock readInit = new BoolLock();

    private MsgReader msgReader;

    private MsgWriter msgWriter;

    private volatile boolean writeDone = false;

    private DefaultMessageStoreImpl defaultMessageStore;

    public MsgReaderInitiator(MsgWriter msgWriter, TIndex index, DefaultMessageStoreImpl defaultMessageStore) {
        msgReader = new MsgReader(index);
        this.msgWriter = msgWriter;
        this.defaultMessageStore = defaultMessageStore;
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
//        if (readInit.tryLock()) {
//            msgWriter.stop();
//            System.out.println("======reader is ready=======");
//            writeDone = true;
//        }
//        while (!writeDone) {
//            try {
//                Thread.sleep(3);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
        defaultMessageStore.setMsgReader(msgReader);
        return msgReader.getMessage(aMin, aMax, tMin, tMax);
    }
}
