package io.openmessaging.store;

import io.openmessaging.buffer.Buffer;
import io.openmessaging.buffer.BufferReader;
import io.openmessaging.common.Const;
import io.openmessaging.util.SimpleThreadLocal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * @author yinjianfeng
 * @date 2019/8/8
 */
public class Vfs {

    private static ExecutorService executorService = Executors.newFixedThreadPool(4);

    public enum VfsEnum {
        //
        body(Const.BODY_SIZE),

        at(Const.A_SIZE);

        public Vfs vfs = new Vfs(this);

        private SimpleThreadLocal<VfsFuture> futureLocal;

        VfsEnum(int bitSize) {
            futureLocal = SimpleThreadLocal.withInitial(() -> new VfsFuture(bitSize));
        }

        public VfsFuture read(long offset, int size) {
            VfsFuture future = futureLocal.get();
            if (Buffer.inBuffer(offset, size) == Buffer.InBuffer.in) {
                future.forceGet().init(offset, size);
                return future;
            }
            future.init(size);
            executorService.submit(
                    () -> {
                        BufferReader bufferReader = future.forceGet();
                        vfs.read(offset, bufferReader);
                        future.done();
                    }
            );
            return future;
        }

        public void write(ByteBuffer src, Consumer<ByteBuffer> consumer) {
            vfs.write(src, consumer);
        }

        public void close() {
            vfs.close();
            vfs = null;
            futureLocal = null;
        }
    }

    private VfsEnum vfsEnum;

    private final static int FILE_SIZE = 30;

    private long writePos;

    private AsynchronousFileChannel asyncFileChannel;

    private FileChannel fileChannel;

    private SimpleThreadLocal<Map<Integer, MappedByteBuffer>> bufferThreadLocal
            = SimpleThreadLocal.withInitial(HashMap::new);

    private void makeSureFile(String fileName){
        File file = new File(fileName);
        try (RandomAccessFile ra = new RandomAccessFile(file, "rw")){
            ra.setLength(1 << FILE_SIZE);
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
                    executorService);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private FileChannel fileChannel() {
        String fileName = Const.DATA_PATH + vfsEnum.name();
        try {
            return FileChannel.open(Paths.get(fileName));
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
            asyncFileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void cache() {
        executorService.submit(
                () -> {
                    //缓存中4g a到内存
                    Buffer.cacheA(fileChannel);
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

    private byte[] readByfileChannel(long offset, int size) {
        try {
            byte[] result = null;
            while (result == null) {
                try {
                    result = new byte[size];
                } catch (OutOfMemoryError e) {
                    try {
                        System.out.println("outOfMemory in VFS.read");
                        Thread.sleep(3);
                    } catch (InterruptedException e1) {
                        //
                    }
                }
            }

            int startNo = (int)(offset >>> FILE_SIZE);
            int endNo = (int)((offset + size) >>> FILE_SIZE);
            int realOffset = (int)(offset - ((long) startNo << FILE_SIZE));
            if (startNo == endNo) {
                ((MappedByteBuffer)mappedByteBuffer(startNo).position(realOffset)).get(result);
            } else {
                int length = (1 << FILE_SIZE) - realOffset;
                ((MappedByteBuffer)mappedByteBuffer(startNo).position(realOffset)).get(result, 0, length);
                for (int i = startNo + 1; i < endNo; i++) {
                    ((MappedByteBuffer)mappedByteBuffer(i).position(0)).get(result, length, 1 << FILE_SIZE);
                    length += (1<<FILE_SIZE);
                }
                ((MappedByteBuffer)mappedByteBuffer(endNo).position(0)).get(result, length, size - length);
            }
            return result;
        } catch (Exception e) {
            System.out.println("read offset:" + offset + "size:" + size + "catch error");
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
