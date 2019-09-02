package io.openmessaging.store;

import io.openmessaging.DefaultMessageStoreImpl;
import io.openmessaging.Message;
import io.openmessaging.common.BoolLock;
import io.openmessaging.index.TIndex;

/**
 * @author yinjianfeng
 * @date 2019/8/31
 */
public class MsgWriterInitiator extends MsgWriter {

    private MsgWriter msgWriter;

    private BoolLock putInit = new BoolLock();

    private DefaultMessageStoreImpl defaultMessageStore;

    public MsgWriterInitiator(TIndex index, DefaultMessageStoreImpl defaultMessageStore) {
        msgWriter = new MsgWriter(index);
        this.defaultMessageStore = defaultMessageStore;
    }

    @Override
    public void put(Message message) {
        msgWriter.put(message);
        if (putInit.tryLock()) {
            msgWriter.start();
        }
        defaultMessageStore.setMsgWriter(msgWriter);
    }

    public MsgWriter getMsgWriter() {
        return msgWriter;
    }
}
