package io.openmessaging.store;

import io.openmessaging.common.Const;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
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

        public void write(ByteBuffer src) {
            vfs.write(src);
        }

    }

    private VfsEnum vfsEnum;

    private final static int FILE_SIZE = 30;

    private long pos;

    private Map<Integer, FileChannel> channelMap = new HashMap<>();

    private ThreadLocal<Map<Integer, MappedByteBuffer>> bufferThreadLocal
            = ThreadLocal.withInitial((Supplier<HashMap<Integer, MappedByteBuffer>>) HashMap::new);

    private FileChannel fileChannel(int no) {
        FileChannel fileChannel = channelMap.get(no);
        if (fileChannel == null) {
            synchronized (vfsEnum) {
                fileChannel = channelMap.get(no);
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

    private void write(ByteBuffer src) {
        try {
            int size = src.limit();
            int startNo = (int) (pos >> FILE_SIZE);
            int endNo = (int) ((pos + size) >> FILE_SIZE);
            if (startNo == endNo) {
                fileChannel(startNo).write(src);
            } else {
                int realOffset = (int) (pos - (startNo << FILE_SIZE));
                int firstLength = (1 << FILE_SIZE) - realOffset;
                src.limit(firstLength);
                fileChannel(startNo).write(src);
                src.limit(size);
                fileChannel(endNo).write(src);
            }
            pos += size;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
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
                int firstLength = (1 << FILE_SIZE) - realOffset;
                ((MappedByteBuffer)mappedByteBuffer(startNo).position(realOffset)).get(result, 0, firstLength);
                for (int i = startNo + 1; i < endNo; i++) {
                    ((MappedByteBuffer)mappedByteBuffer(i).position(0)).get(result,
                            firstLength + (i - startNo - 1) * (1 << FILE_SIZE), 1 << FILE_SIZE);
                }
                ((MappedByteBuffer)mappedByteBuffer(endNo).position(0)).get(result,
                        firstLength + (endNo - startNo - 1) * (1 << FILE_SIZE), size - (endNo - startNo - 1) * (1 << FILE_SIZE) - firstLength);
            }
            return result;
        } catch (Exception e) {
            System.out.println("read offset:" + offset + "size:" + size + "catch error");
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

}
