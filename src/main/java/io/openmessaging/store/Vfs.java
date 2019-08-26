package io.openmessaging.store;

import io.openmessaging.common.Const;

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
import java.util.function.Supplier;

/**
 * @author yinjianfeng
 * @date 2019/8/8
 */
public class Vfs {

    enum VfsEnum {
        //
        body,

        // a t a t a t
        at;

        public Vfs vfs = new Vfs(this);

        public byte[] read(long offset, int size) {
            return vfs.read(offset, size);
        }

        public void write(ByteBuffer src, Consumer<ByteBuffer> consumer) {
            vfs.write(src, consumer);
        }

    }

    private VfsEnum vfsEnum;

    private final static int FILE_SIZE = 30;

    private long pos;

    private volatile Map<Integer, Object> channelMap = new HashMap<>();

    private Map<Integer, AsynchronousFileChannel> asyncChannelMap = new HashMap<>();

    private ThreadLocal<Map<Integer, MappedByteBuffer>> bufferThreadLocal
            = ThreadLocal.withInitial((Supplier<HashMap<Integer, MappedByteBuffer>>) HashMap::new);

    private static ExecutorService executorService = Executors.newSingleThreadExecutor();

    void makeSureFile(String fileName){
        File file = new File(fileName);
        try (RandomAccessFile ra = new RandomAccessFile(file, "rw")){
            ra.setLength(1 << FILE_SIZE);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private AsynchronousFileChannel asyncFileChannel(int no) {
        AsynchronousFileChannel fileChannel = (AsynchronousFileChannel) channelMap.get(no);
        if (fileChannel == null) {
            synchronized (vfsEnum) {
                fileChannel = (AsynchronousFileChannel) channelMap.get(no);
                if (fileChannel == null) {
                    String fileName = Const.DATA_PATH + vfsEnum.name() + no;
                    try {
                        Path path = Paths.get(fileName);
                        makeSureFile(fileName);
                        fileChannel = AsynchronousFileChannel.open(path,
                                new HashSet<OpenOption>(Collections.singletonList(StandardOpenOption.WRITE)),
                                executorService);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    channelMap.put(no, fileChannel);
                }
            }
        }
        return fileChannel;
    }

    private FileChannel fileChannel(int no) {
        System.out.println("size:" + channelMap.size() + " in " + Thread.currentThread().getName());
        FileChannel fileChannel = (FileChannel) channelMap.get(no);
        if (fileChannel == null) {
            synchronized (vfsEnum) {
                fileChannel = (FileChannel) channelMap.get(no);
                if (fileChannel == null) {
                    String fileName = Const.DATA_PATH + vfsEnum.name() + no;
                    try {
                        File file = new File(fileName);
                        fileChannel = new RandomAccessFile(file, "rw").getChannel();
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    channelMap.put(no, fileChannel);
                }
            }
        }
        return fileChannel;
    }

    private MappedByteBuffer mappedByteBuffer(int no) {
        MappedByteBuffer buffer = bufferThreadLocal.get().get(no);
        if (buffer == null) {
            synchronized (vfsEnum) {
                buffer = bufferThreadLocal.get().get(no);
                if (buffer == null) {
                    try {
                        buffer = fileChannel(no).map(FileChannel.MapMode.READ_ONLY, 0, 1 << FILE_SIZE);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    bufferThreadLocal.get().put(no, buffer);
                }
            }
        }
        return buffer;
    }

    private Vfs(VfsEnum vfsEnum) {
        this.vfsEnum = vfsEnum;
    }

    private void write(ByteBuffer src, Consumer<ByteBuffer> consumer) {
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
            int size = src.limit();
            int startNo = (int) (pos >> FILE_SIZE);
            int endNo = (int) ((pos + size) >> FILE_SIZE);
            if (startNo == endNo) {
                asyncFileChannel(startNo).write(src, pos, null, handler);
            } else {
                int realOffset = (int) (pos - (startNo << FILE_SIZE));
                int firstLength = (1 << FILE_SIZE) - realOffset;
                src.limit(firstLength);
                asyncFileChannel(startNo).write(src.slice(), pos);
                src.position(firstLength);
                src.limit(size);
                asyncFileChannel(endNo).write(src, pos, null, handler);
            }
            pos += size;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void writeDone() {
        for (Object value : channelMap.values()) {
            try {
                ((AsynchronousFileChannel) value).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        channelMap.clear();
    }

    private byte[] read(long offset, int size) {
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

            int startNo = (int)(offset >> FILE_SIZE);
            int endNo = (int)((offset + size) >> FILE_SIZE);
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

}
