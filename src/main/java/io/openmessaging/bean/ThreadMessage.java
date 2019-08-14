package io.openmessaging.bean;

import io.openmessaging.Message;
import io.openmessaging.common.Const;
import io.openmessaging.util.Ring;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author yinjianfeng
 * @date 2019/8/3
 */
public class ThreadMessage {

    public final static List<ThreadMessage> TM = new LinkedList<>();

    private volatile Message minMessage;

    private final Ring<Message> messages = new Ring<>(new Message[Const.MAX_PUT_SIZE]);

    private final static Ring<Message> DUMP_MESSAGES = new Ring<>(new Message[Const.MAX_DUMP_SIZE]);

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

    private Message surePop() {
        final Message pop = minMessage;
        try {
            minMessage = messages.pop();
        } catch (Exception e) {
            if (!(e instanceof NoSuchElementException)) {
                e.printStackTrace();
            } else {
                minMessage = null;
            }
        }
        return pop;
    }

    private Message getMinMessage() {
        return minMessage;
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

    public static Ring<Message> dumpStoreMsg(long endT) {
        final List<ThreadMessage> list = new ArrayList<>(ThreadMessage.TM);
        while (list.size() > 0 && !DUMP_MESSAGES.isFull()) {
            long minT = Long.MAX_VALUE;
            ThreadMessage minMessage = null;
            for (int i = list.size() - 1; i >= 0; i--) {
                ThreadMessage threadMessage = list.get(i);

                final Message message = threadMessage.getMinMessage();

                if (message == null || message.getT() > endT) {
                    list.remove(i);
                    continue;
                }
                if (message.getT() < minT) {
                    minT = message.getT();
                    minMessage = threadMessage;
                }
            }
            if (minMessage != null) {
                DUMP_MESSAGES.add(minMessage.surePop());
            }
        }
        return DUMP_MESSAGES;
    }

}
