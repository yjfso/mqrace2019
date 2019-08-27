package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.bean.ThreadMessageManager;
import io.openmessaging.common.Const;
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

    private Ring<ByteBuffer> aBufferRing = new Ring<>(new ByteBuffer[Const.WRITE_ASYNC_NUM])
            .fill(() -> DirectBuffer.ask(8 * Const.MAX_DUMP_SIZE));

    private Ring<ByteBuffer> bodyBufferRing = new Ring<>(new ByteBuffer[Const.WRITE_ASYNC_NUM])
            .fill(() -> DirectBuffer.ask(Const.BODY_SIZE * Const.MAX_DUMP_SIZE));

    private int msgNum = 0;

    private TIndex index;

    private long lastT;

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
        ByteBuffer atBuffer = aBufferRing.popWait();
        ByteBuffer bodyBuffer = bodyBufferRing.popWait();

        while ((message = messages.pop()) != null) {
            index.put(message.getT());
            try {
                if (lastT != message.getT()) {
                    atBuffer.putLong(message.getT());
                }
                atBuffer.putLong(message.getA());
                bodyBuffer.put(message.getBody());
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (msgNum++ == 0) {
                System.out.println("put first t:" + message.getT() + "; a: " + message.getA() + " at " + System.currentTimeMillis());
            }
        }
        atBuffer.flip();
        atFile.write(atBuffer, e -> {
            aBufferRing.add1(e);
            e.clear();
        });
        bodyBuffer.flip();
        bodyFile.write(bodyBuffer, e -> {
            bodyBufferRing.add1(e);
            e.clear();
        });
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
        thread.setPriority(Thread.MAX_PRIORITY);
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
        while (!aBufferRing.isFull()) {
            try {
                Thread.sleep(1);
                System.out.println("wait for write done...");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Vfs.VfsEnum.at.vfs.writeDone();
        Vfs.VfsEnum.body.vfs.writeDone();
        aBufferRing = null;
        bodyBufferRing = null;
        DirectBuffer.returnAll();
    }

}
