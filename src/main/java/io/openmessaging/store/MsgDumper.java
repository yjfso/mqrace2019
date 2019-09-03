package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.bean.ThreadMessageManager;
import io.openmessaging.buffer.ABuffer;
import io.openmessaging.common.Const;
import io.openmessaging.index.TIndex;
import io.openmessaging.util.ConcurrentWriteRing;
import io.openmessaging.util.Ring;
import io.openmessaging.util.UnsafeHolder;

import java.nio.ByteBuffer;

import static io.openmessaging.common.Common.EXECUTOR_SERVICE;
import static io.openmessaging.common.Const.ARRAY_BASE_OFFSET;
import static io.openmessaging.common.Const.BODY_SIZE;
import static io.openmessaging.util.UnsafeHolder.UNSAFE;

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
            .fill(() -> ABuffer.requireDirect(BODY_SIZE * Const.MAX_DUMP_SIZE));

    public MsgDumper(TIndex index, ThreadMessageManager threadMessageManager) {
        this.index = index;
        this.threadMessageManager = threadMessageManager;
    }

    private void writeToFile(ByteBuffer byteBuffer, Vfs.VfsEnum vfsEnum, ConcurrentWriteRing<ByteBuffer> ring) {
        byteBuffer.flip();
        vfsEnum.write(byteBuffer, e -> {
            ring.threadSafeAdd(e);
            e.clear();
        });
        byteBuffer.clear();
    }

    private void write(Ring<Message> messages) {
        if (messages.isEmpty()) {
            return;
        }
        Message message;
        ByteBuffer atBuffer = aBufferRing.popWait();
        ByteBuffer bodyBuffer = bodyBufferRing.popWait();

        long atAddress = UNSAFE.getLong(atBuffer, Const.BUFFER_ADDRESS_OFFSET);
        long bodyAddress = UNSAFE.getLong(bodyBuffer, Const.BUFFER_ADDRESS_OFFSET);
        int i = 0;
        while ((message = messages.pop()) != null) {
            index.put(message.getT());
            try {
                UNSAFE.putLong(atAddress, Long.reverseBytes(message.getA()));
                UnsafeHolder.UNSAFE.copyMemory(message.getBody(), ARRAY_BASE_OFFSET, null, bodyAddress, BODY_SIZE);
                bodyAddress += BODY_SIZE;
                atAddress += 8;
                i ++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        atBuffer.position(i * 8);
        bodyBuffer.position(i * BODY_SIZE);
        writeToFile(atBuffer, atFile, aBufferRing);
        writeToFile(bodyBuffer, bodyFile, bodyBufferRing);
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
        ABuffer.writeDone(index.writeDone());

        EXECUTOR_SERVICE.submit(
                () -> {
                    try {
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
                        System.out.println("=====write done======");
                        Vfs.VfsEnum.at.vfs.cache();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );

    }
}
