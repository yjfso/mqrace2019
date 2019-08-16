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

    private final static List<ThreadMessage> TM = new ArrayList<>();

    private final static Ring<Message> DUMP_MESSAGES = new Ring<>(new Message[Const.MAX_DUMP_SIZE]);

    volatile static boolean stop = false;

    public static void register(ThreadMessage threadMessage) {
        synchronized (TM) {
            TM.add(threadMessage);
        }
    }

    private static StreamLoserTree<ThreadMessage, Message> streamLoserTree;

    public static void initDump() {
        streamLoserTree = new StreamLoserTree<>(TM);
    }

    public static void finishDump() {
        stop = true;
        streamLoserTree.setEnd(true);
    }

    public static Ring<Message> dumpStoreMsg() {
        while (!DUMP_MESSAGES.isFull()) {
            Message message = streamLoserTree.askWinner();
            if (message == null) {
                break;
            }
            DUMP_MESSAGES.add(message);
        }
        return DUMP_MESSAGES;
    }
}
