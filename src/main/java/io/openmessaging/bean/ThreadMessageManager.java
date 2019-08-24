package io.openmessaging.bean;

import io.openmessaging.Message;
import io.openmessaging.common.Const;
import io.openmessaging.util.Ring;
import io.openmessaging.util.StreamLoserTree;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yinjianfeng
 * @date 2019/8/16
 */
public class ThreadMessageManager {

    private final List<ThreadMessage> TM = new ArrayList<>();

    private final Ring<Message> DUMP_MESSAGES = new Ring<>(new Message[Const.MAX_DUMP_SIZE]);

    public void register(ThreadMessage threadMessage) {
        synchronized (TM) {
            TM.add(threadMessage);
        }
    }

    private StreamLoserTree<ThreadMessage, Message> streamLoserTree;

    public void init() {
        streamLoserTree = new StreamLoserTree<>(TM);
    }

    public Ring<Message> dumpStoreMsg() {
        while (!DUMP_MESSAGES.isFull()) {
            Message message = streamLoserTree.askWinner();
            if (message == null) {
                break;
            }
            DUMP_MESSAGES.add(message);
        }
        return DUMP_MESSAGES;
    }

    public List<ThreadMessage> getTM() {
        return TM;
    }
}
