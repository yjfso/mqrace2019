package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.bean.ThreadMessageManager;
import io.openmessaging.common.Const;
import io.openmessaging.index.DichotomicIndex;
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

    private long lastT;

    private DichotomicIndex index;

    private ThreadMessageManager threadMessageManager;

    public MsgWriter(DichotomicIndex index, ThreadMessageManager threadMessageManager){
        this.index = index;
        this.threadMessageManager = threadMessageManager;
    }

    private void write(Ring<Message> messages) {
        if (messages.isEmpty()) {
            return;
        }
        boolean needIndex = false;
        Message message;
        while ((message = messages.pop()) != null) {
            if (message.getT() < lastT) {
                System.out.println("=============error=============");
                System.out.println(message.getT() + "<" + lastT);
            }
            if (lastT != message.getT()) {
                if (needIndex || (msgNum & Const.INDEX_INTERVAL) == 0) {
                    index.put(message.getT(), msgNum);
                    needIndex = false;
                }
            } else if((msgNum & Const.INDEX_INTERVAL) == 0) {
                needIndex = true;
            }
            lastT = message.getT();
            try {
                atBuffer.putLong(message.getA());
                atBuffer.putLong(message.getT());
            } catch (Exception e) {
                e.printStackTrace();
            }
            bodyBuffer.put(message.getBody());
            msgNum++;
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
                System.out.println("put last t:" + last.getT() + "; a: " + last.getA() + " total num:" + msgNum);
                index.put(last.getT(), msgNum);
                index.dumpInfo();
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
        System.out.println("=====write done======");
        atBuffer = null;
        bodyBuffer = null;
    }

}
