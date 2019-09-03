package io.openmessaging.store;

import io.openmessaging.buffer.ABuffer;
import io.openmessaging.buffer.BufferReader;
import io.openmessaging.common.Const;
import io.openmessaging.util.SimpleThreadLocal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
                vfs.readByFileChannel(offset, bufferReader);
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

    private final static int FILE_SIZE = 30;

    private boolean writing = true;

    private long writePos;

    private AsynchronousFileChannel asyncFileChannel;

    private FileChannel fileChannel;

    private SimpleThreadLocal<FileChannel> fileChannelLocal = SimpleThreadLocal.withInitial(this::fileChannel);

    private SimpleThreadLocal<Map<Integer, MappedByteBuffer>> bufferThreadLocal
            = SimpleThreadLocal.withInitial(HashMap::new);

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

    private MappedByteBuffer mappedByteBuffer(int no) {
        MappedByteBuffer buffer = bufferThreadLocal.get().get(no);
        if (buffer == null) {
            try {
                long startPos = ((long)no) << FILE_SIZE;
                buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPos, Math.min(1 << FILE_SIZE, writePos - startPos));
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            bufferThreadLocal.get().put(no, buffer);
        }
        return buffer;
    }

    private Vfs(VfsEnum vfsEnum) {
        this.vfsEnum = vfsEnum;
        asyncFileChannel = asyncFileChannel();
        fileChannel = fileChannel();
    }

    private void write(ByteBuffer src, Consumer<ByteBuffer> consumer) {
        //todo
        CompletionHandler<Integer, Object> handler = new CompletionHandler<Integer, Object>(){

            @Override
            public void completed(Integer result, Object attachment) {
                consumer.accept(src);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                consumer.accept(src);
                exc.printStackTrace();
            }
        };

        try {
            final int size = src.limit();
            asyncFileChannel.write(src, writePos, null, handler);
            writePos += size;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void writeDone() {
        try {
            writing = false;
            asyncFileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void cache() {
        EXECUTOR_SERVICE.submit(
                () -> {
                    //缓存中4g a到内存
                    ABuffer.cacheA(fileChannel());
                }
        );
    }

    private void read(long offset, BufferReader bufferReader) {
        int size = bufferReader.getSize();
        try {
            int startNo = (int)(offset >>> FILE_SIZE);
            int endNo = (int)((offset + size) >>> FILE_SIZE);
            int realOffset = (int)(offset - ((long) startNo << FILE_SIZE));
            if (startNo == endNo) {
                ((MappedByteBuffer)mappedByteBuffer(startNo).position(realOffset)).get(bufferReader.getBytes(), 0 , size);
            } else {
                int length = (1 << FILE_SIZE) - realOffset;
                ((MappedByteBuffer)mappedByteBuffer(startNo).position(realOffset)).get(bufferReader.getBytes(), 0, length);
                for (int i = startNo + 1; i < endNo; i++) {
                    ((MappedByteBuffer)mappedByteBuffer(i).position(0)).get(bufferReader.getBytes(), length, 1 << FILE_SIZE);
                    length += (1<<FILE_SIZE);
                }
                ((MappedByteBuffer)mappedByteBuffer(endNo).position(0)).get(bufferReader.getBytes(), length, size - length);
            }
        } catch (Exception e) {
            System.out.println("read offset:" + offset + "size:" + size + "catch error");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void readByFileChannel(long offset, BufferReader bufferReader) {
        try {
            ByteBuffer byteBuffer = bufferReader.getByteBuffer();
            if (writing && ((byteBuffer.limit() + offset) > writePos)) {
                try {
                    Thread.sleep(1);
                    System.out.println("read from file, but writing unfinished...");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            fileChannelLocal.get().position(offset).read(byteBuffer);
            bufferReader.getByteBuffer().flip();
//            int startNo = (int)(offset >>> FILE_SIZE);
//            int endNo = (int)((offset + size) >>> FILE_SIZE);
//            int realOffset = (int)(offset - ((long) startNo << FILE_SIZE));
//            if (startNo == endNo) {
//                ((MappedByteBuffer)mappedByteBuffer(startNo).position(realOffset)).get(bufferReader.getBytes(), 0 , size);
//            } else {
//                int length = (1 << FILE_SIZE) - realOffset;
//                ((MappedByteBuffer)mappedByteBuffer(startNo).position(realOffset)).get(bufferReader.getBytes(), 0, length);
//                for (int i = startNo + 1; i < endNo; i++) {
//                    ((MappedByteBuffer)mappedByteBuffer(i).position(0)).get(bufferReader.getBytes(), length, 1 << FILE_SIZE);
//                    length += (1<<FILE_SIZE);
//                }
//                ((MappedByteBuffer)mappedByteBuffer(endNo).position(0)).get(bufferReader.getBytes(), length, size - length);
//            }
        } catch (Exception e) {
            System.out.println("read offset:" + offset + "size:" + bufferReader.getSize() + "catch error");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void close() {
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
