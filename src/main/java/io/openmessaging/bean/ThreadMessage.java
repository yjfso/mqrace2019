package io.openmessaging.bean;

import io.openmessaging.Message;
import io.openmessaging.common.Const;
import io.openmessaging.util.Ring;
import io.openmessaging.util.StreamTreeNode;

/**
 * @author yinjianfeng
 * @date 2019/8/3
 */
public class ThreadMessage implements StreamTreeNode<ThreadMessage, Message> {

    private volatile Message minMessage;

    private final Ring<Message> messages = new Ring<>(new Message[Const.MAX_PUT_SIZE]);

    private final Thread thread = Thread.currentThread();

    public ThreadMessage(ThreadMessageManager threadMessageManager) {
        threadMessageManager.register(this);
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
        while (newMsg == null && thread.isAlive()) {
            try {
                Thread.sleep(10);
                System.out.println("pop wait..." + messages + thread.getName() + ";" + thread.isAlive());
                newMsg = messages.pop();
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
    public boolean lessAndEqual(ThreadMessage o) {
        final Message message = minMessage;
        if (message == null) {
            return true;
        }
        final Message message1 = o.getMinMessage();
        if (message1 == null) {
            return false;
        }
        return message1.getT() > message.getT();
    }

    @Override
    public String toString() {
        return "threadMessage[" + thread.getName() + "]";
    }
}
