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

    private volatile MsgDumper msgDumper;

    private Thread thread;

    private ThreadMessageManager threadMessageManager = new ThreadMessageManager();

    private TIndex index;

    MsgWriter() {
    }

    MsgWriter(TIndex index){
        messages = SimpleThreadLocal.withInitial(
                () -> new ThreadMessage(threadMessageManager)
        );
        this.index = index;
    }

    public void put(Message message) {
        messages.get().put(message);
    }

    public void start() {
        thread = new Thread(() -> {
            index.init();
            msgDumper = new MsgDumper(index, threadMessageManager);
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
