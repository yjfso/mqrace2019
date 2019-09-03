package io.openmessaging.store;

import io.openmessaging.buffer.ABuffer;
import io.openmessaging.buffer.BufferReader;
import io.openmessaging.common.Const;
import io.openmessaging.util.SimpleThreadLocal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.function.Consumer;

import static io.openmessaging.common.Common.EXECUTOR_SERVICE;

/**
 * @author yinjianfeng
 * @date 2019/8/8
 */
public class Vfs {

    public enum VfsEnum {
        //
        body(Const.BODY_SIZE),

        at(Const.A_SIZE) {

            @Override
            public boolean inBuffer(long offset, int size) {
                return ABuffer.inBuffer(offset, size) == ABuffer.InBuffer.in;
            }
        };

        public Vfs vfs = new Vfs(this);

        private SimpleThreadLocal<BufferReader> bufferReaderLocal;

        VfsEnum(int bitSize) {
            bufferReaderLocal = SimpleThreadLocal.withInitial(() -> new BufferReader(bitSize));
        }

        boolean inBuffer(long offset, int size) {
            return false;
        }

        public BufferReader read(long offset, int size) {
            BufferReader bufferReader = bufferReaderLocal.get();
            if (inBuffer(offset, size)) {
                bufferReader.initFromBuffer(offset);
            } else {
                bufferReader.init(size);
                vfs.readByFileChannel(offset, bufferReader.getByteBuffer());
            }
//            executorService.submit(
//                    () -> {
//                        BufferReader bufferReader = future.forceGet();
//                        vfs.readByFileChannel(offset, bufferReader);
//                        future.done();
//                    }
//            );
            return bufferReader;
        }

        public void write(ByteBuffer src, Consumer<ByteBuffer> consumer) {
            vfs.write(src, consumer);
        }

        public static void getMsgDone() {
            body.vfs.close();
            body.vfs = null;
            body.bufferReaderLocal = null;

            at.bufferReaderLocal = SimpleThreadLocal.withInitial(() -> new BufferReader(Const.A_SIZE));
            EXECUTOR_SERVICE.submit(ABuffer::getMessageDone);
        }
    }

    private VfsEnum vfsEnum;

    private boolean writing = true;

    private long writePos;

    private volatile long writeDonePos;

    private AsynchronousFileChannel asyncFileChannel;

    private FileChannel fileChannel;

    private SimpleThreadLocal<FileChannel> fileChannelLocal = SimpleThreadLocal.withInitial(this::fileChannel);

    private void makeSureFile(String fileName){
        try {
            File file = new File(fileName);
            if (!file.exists()) {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private AsynchronousFileChannel asyncFileChannel() {
        String fileName = Const.DATA_PATH + vfsEnum.name();
        try {
            Path path = Paths.get(fileName);
            makeSureFile(fileName);
            return AsynchronousFileChannel.open(path,
                    new HashSet<OpenOption>(Collections.singletonList(StandardOpenOption.WRITE)),
                    EXECUTOR_SERVICE);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private FileChannel fileChannel() {
        String fileName = Const.DATA_PATH + vfsEnum.name();
        try {
            return FileChannel.open(Paths.get(fileName), StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Vfs(VfsEnum vfsEnum) {
        this.vfsEnum = vfsEnum;
        asyncFileChannel = asyncFileChannel();
        fileChannel = fileChannel();
    }

    private void write(ByteBuffer src, Consumer<ByteBuffer> consumer) {
        try {
            final int size = src.limit();
            asyncFileChannel.write(src, writePos, null,
                    new CompletionHandler<Integer, Object>(){

                        @Override
                        public void completed(Integer result, Object attachment) {
                            writeDonePos = Math.max(writePos + size, writeDonePos);
                            consumer.accept(src);
                        }

                        @Override
                        public void failed(Throwable exc, Object attachment) {
                            consumer.accept(src);
                            exc.printStackTrace();
                        }
                    });
            writePos += size;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    void writeDone() {
        try {
            writing = false;
            asyncFileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void cache() {
        EXECUTOR_SERVICE.submit(
                () -> {
                    //缓存中4g a到内存
                    ABuffer.cacheA(fileChannel());
                }
        );
    }
    private void readByFileChannel(long offset, ByteBuffer byteBuffer) {
        try {
            while (writing && ((byteBuffer.limit() + offset) > writeDonePos)) {
                try {
                    Thread.sleep(1);
                    System.out.println("read from file, but writing unfinished...");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            fileChannelLocal.get().position(offset).read(byteBuffer);
            while (byteBuffer.hasRemaining()) {
                try {
                    Thread.sleep(30);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("read 0...");
                byteBuffer.position(0);
                fileChannelLocal.get().position(offset).read(byteBuffer);
            }
            byteBuffer.flip();
        } catch (Exception e) {
            System.out.println("read offset:" + offset + "size:" + byteBuffer.limit() + "catch error");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void close() {
        try {
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setWritePos(long writePos) {
        this.writePos = writePos;
    }
}
