package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.bean.ThreadMessageManager;
import io.openmessaging.buffer.ABuffer;
import io.openmessaging.common.Const;
import io.openmessaging.index.TIndex;
import io.openmessaging.util.ConcurrentWriteRing;
import io.openmessaging.util.Ring;

import java.nio.ByteBuffer;

/**
 * @author yinjianfeng
 * @date 2019/9/1
 */
public class MsgDumper {

    private TIndex index;

    private ThreadMessageManager threadMessageManager;

    private Vfs.VfsEnum bodyFile = Vfs.VfsEnum.body;

    private Vfs.VfsEnum atFile = Vfs.VfsEnum.at;

    private ConcurrentWriteRing<ByteBuffer> aBufferRing = new ConcurrentWriteRing<>(new ByteBuffer[Const.WRITE_ASYNC_NUM])
            .fill(() -> ABuffer.requireDirect(8 * Const.MAX_DUMP_SIZE));

    private ConcurrentWriteRing<ByteBuffer> bodyBufferRing = new ConcurrentWriteRing<>(new ByteBuffer[Const.WRITE_ASYNC_NUM])
            .fill(() -> ABuffer.requireDirect(Const.BODY_SIZE * Const.MAX_DUMP_SIZE));

    public MsgDumper(TIndex index, ThreadMessageManager threadMessageManager) {
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
                atBuffer.putLong(message.getA());
                bodyBuffer.put(message.getBody());
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        atBuffer.flip();
        atFile.write(atBuffer, e -> {
            aBufferRing.threadSafeAdd(e);
            e.clear();
        });
        bodyBuffer.flip();
        bodyFile.write(bodyBuffer, e -> {
            bodyBufferRing.threadSafeAdd(e);
            e.clear();
        });
        atBuffer.clear();
        bodyBuffer.clear();
    }

    void write() {
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
        System.out.println("put last t:" + last.getT() + "; a: " + last.getA() + " total num:" + index.getNo()
                + " at " + System.currentTimeMillis());
    }

    void writeDone () {
        while (!aBufferRing.isFull() || !bodyBufferRing.isFull()) {
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
        ABuffer.writeDone(index.writeDone());
        Vfs.VfsEnum.at.vfs.cache();
        System.out.println("=====write done======");
    }
}
