package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.bean.ThreadMessage;
import io.openmessaging.bean.ThreadMessageManager;
import io.openmessaging.index.TIndex;
import io.openmessaging.util.SimpleThreadLocal;

/**
 * @author yinjianfeng
 * @date 2019/8/3
 */
public class MsgWriter {

    private SimpleThreadLocal<ThreadMessage> messages;

    private MsgDumper msgDumper;

    private Thread thread;

    MsgWriter() {
    }

    MsgWriter(TIndex index){
        ThreadMessageManager threadMessageManager = new ThreadMessageManager();
        messages = SimpleThreadLocal.withInitial(
                () -> new ThreadMessage(threadMessageManager)
        );
        msgDumper = new MsgDumper(index, threadMessageManager);
    }

    public void put(Message message) {
        messages.get().put(message);
    }

    public void start() {
        thread = new Thread(() -> {
            try {
                msgDumper.write();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "writer...");
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.start();
    }

    public void stop() {
        try{
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        thread = null;
        messages = null;
        msgDumper.writeDone();
    }

}
