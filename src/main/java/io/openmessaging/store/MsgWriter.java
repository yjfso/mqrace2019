package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.bean.ThreadMessageManager;
import io.openmessaging.index.TIndex;
import io.openmessaging.util.Ring;

import java.nio.ByteBuffer;

/**
 * @author yinjianfeng
 * @date 2019/8/3
 */
public class MsgWriter {

    private Vfs.VfsEnum bodyFile = Vfs.VfsEnum.body;

    private Vfs.VfsEnum atFile = Vfs.VfsEnum.at;

    private ByteBuffer atBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 500);

    private ByteBuffer bodyBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 1024);

    private int msgNum = 0;

    private TIndex index;

    private ThreadMessageManager threadMessageManager;

    public MsgWriter(TIndex index, ThreadMessageManager threadMessageManager){
        this.index = index;
        this.threadMessageManager = threadMessageManager;
    }

    private void write(Ring<Message> messages) {
        if (messages.isEmpty()) {
            return;
        }
        Message message;
        while ((message = messages.pop()) != null) {
            index.put(message.getT());
            try {
                atBuffer.putLong(message.getA());
            } catch (Exception e) {
                e.printStackTrace();
            }
            bodyBuffer.put(message.getBody());
            if (msgNum++ == 0) {
                System.out.println("put first t:" + message.getT() + "; a: " + message.getA() + " at " + System.currentTimeMillis());
            }
        }
        atBuffer.flip();
        atFile.write(atBuffer);
        bodyBuffer.flip();
        bodyFile.write(bodyBuffer);
        atBuffer.clear();
        bodyBuffer.clear();
    }

    private Thread thread;

    private Message write() {
        threadMessageManager.init();
        Message last = null;
        while (true) {
            Ring<Message> messages = threadMessageManager.dumpStoreMsg();
            if (messages.isEmpty()) {
                break;
            }
            last = messages.getLast();
            write(messages);
        }
        return last;
    }

    @SuppressWarnings("AlibabaAvoidManuallyCreateThread")
    public void start() {
        thread = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                Message last = write();
                System.out.println("put last t:" + last.getT() + "; a: " + last.getA() + " total num:" + msgNum + " at " + System.currentTimeMillis());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "writer...");
//        thread.setPriority(MAX_PRIORITY);
        thread.start();
    }

    public void stop() {
        try{
            thread.join();
            thread = null;
            threadMessageManager = null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        index.writeDone();
        System.out.println("=====write done======");
        atBuffer = null;
        bodyBuffer = null;
    }

}
