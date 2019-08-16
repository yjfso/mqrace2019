package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.bean.ThreadMessageManager;
import io.openmessaging.common.Const;
import io.openmessaging.index.DichotomicIndex;
import io.openmessaging.util.Ring;

import java.nio.ByteBuffer;

import static java.lang.Thread.MAX_PRIORITY;

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

    private long maxDiff = 0;

    private long minDiff = Long.MAX_VALUE;

    public MsgWriter(DichotomicIndex index){
        this.index = index;
    }

    private void write(Ring<Message> messages) {
        if (messages.isEmpty()) {
            return;
        }
        boolean needIndex = false;
        Message message;
        while ((message = messages.pop()) != null) {
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
                maxDiff = Math.max(message.getA() - message.getT(), maxDiff);
                minDiff = Math.min(message.getA() - message.getT(), minDiff);
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
        Message last = null;
        while (true) {
            Ring<Message> messages = ThreadMessageManager.dumpStoreMsg();
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
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
            try {
                ThreadMessageManager.initDump();
                Message last = write();
                System.out.println("put last t:" + last.getT() + "; a: " + last.getA() + " total num:" + msgNum);
                index.put(last.getT(), msgNum);
                index.dumpInfo();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }, "writer");
//        thread.setPriority(MAX_PRIORITY);
        thread.start();
    }

    public void stop() {
        try{
            ThreadMessageManager.finishDump();
            thread.join();
            thread = null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("maxDiff:" + maxDiff + ";minDiff:" + minDiff);
        atBuffer = null;
        bodyBuffer = null;
    }

}
