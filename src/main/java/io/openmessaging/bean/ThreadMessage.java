package io.openmessaging.bean;

import io.openmessaging.Message;
import io.openmessaging.common.Const;
import io.openmessaging.util.Ring;
import io.openmessaging.util.StreamLoserTree;
import io.openmessaging.util.StreamTreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yinjianfeng
 * @date 2019/8/3
 */
public class ThreadMessage implements StreamTreeNode<ThreadMessage, Message> {

    private final static List<ThreadMessage> TM = new ArrayList<>();

    private volatile Message minMessage;

    private final Ring<Message> messages = new Ring<>(new Message[Const.MAX_PUT_SIZE]);

    private final static Ring<Message> DUMP_MESSAGES = new Ring<>(new Message[Const.MAX_DUMP_SIZE]);

    private volatile static boolean stop = false;

    public ThreadMessage() {
        synchronized (TM) {
            TM.add(this);
        }
    }

    public void put(Message message) {
        if (minMessage == null) {
            minMessage = message;
        } else {
            this.messages.add(message);
        }
    }

    @Override
    public Message pop() {
        final Message pop = minMessage;
        Message newMsg = messages.pop();
        while (!stop && newMsg == null) {
            try {
                newMsg = messages.pop();
                Thread.sleep(3);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        minMessage = newMsg;
        return pop;
    }

    private Message getMinMessage() {
        return minMessage;
    }

    @Override
    public boolean isEmpty() {
        return minMessage == null;
    }

    @Override
    public int compareTo(ThreadMessage o) {
        final Message message = minMessage;
        if (message == null) {
            return -1;
        }
        final Message message1 = o.getMinMessage();
        if (message1 == null) {
            return 1;
        }
        return (int)(message1.getT() - message.getT());
    }

    private static StreamLoserTree<ThreadMessage, Message> streamLoserTree;

    public static void initDump() {
        streamLoserTree = new StreamLoserTree<>(ThreadMessage.TM);
    }

    public static void finishDump() {
        stop = true;
        streamLoserTree.setEnd(true);
    }

    public static long minTInAll() {
        long minT = Long.MAX_VALUE;
        for (ThreadMessage threadMessage : ThreadMessage.TM) {
            Message message = threadMessage.messages.getLast();
            if (message != null) {
                minT = Math.min(minT, message.getT());
            }
        }
        System.out.println("minT:" + minT);
        return minT;
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
//        final List<ThreadMessage> list = new ArrayList<>(ThreadMessage.TM);
//        while (list.size() > 0 && !DUMP_MESSAGES.isFull()) {
//            long minT = Long.MAX_VALUE;
//            ThreadMessage minMessage = null;
//            for (int i = list.size() - 1; i >= 0; i--) {
//                ThreadMessage threadMessage = list.get(i);
//
//                final Message message = threadMessage.getMinMessage();
//
//                if (message == null || message.getT() > endT) {
//                    list.remove(i);
//                    continue;
//                }
//                if (message.getT() < minT) {
//                    minT = message.getT();
//                    minMessage = threadMessage;
//                }
//            }
//            if (minMessage != null) {
//                DUMP_MESSAGES.add(minMessage.surePop());
//            }
//        }
//        return DUMP_MESSAGES;
    }

    @Override
    public String toString() {
        return "(T:" +String.valueOf(minMessage.getT()) + ")";
    }
}
