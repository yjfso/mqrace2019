package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.bean.ThreadMessage;
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

    private volatile boolean isStop = false;

    private long maxDiff = 0;

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

    private Message write(long endT) {
        Message last = null;
        while (true) {
            Ring<Message> messages = ThreadMessage.dumpStoreMsg(endT);
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
            while (true) {
                try {
                    try {
                        Thread.sleep(3);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return;
                    }
                    if (isStop) {
                        System.out.println("stop writer...");
                        return;
                    }
                    write(ThreadMessage.minTInAll());

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }, "writer");
        thread.setPriority(MAX_PRIORITY);
        thread.start();

    }

    public void stop() {
        isStop = true;
        try{
            thread.join();
            thread = null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Message last = write(Long.MAX_VALUE);
        System.out.println("put last t:" + last.getT() + "; a: " + last.getA() + " total num:" + msgNum);
        index.put(last.getT(), msgNum);
        System.out.println("maxDiff:" + maxDiff);

        atBuffer = null;
        bodyBuffer = null;
    }

}
