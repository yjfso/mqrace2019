package io.openmessaging;

import io.openmessaging.common.Const;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static java.util.Collections.EMPTY_LIST;

/**
 * @author yinjianfeng
 * @date 2019/8/26
 */
public class FileReadTester {

    static class Meta {
        long start;
        int length;
        public Meta(long start, int length) {
            this.start = start;
            this.length = length;
        }
    }

    private static int readNum = 100000;

    private static int threadNum = 4;

    private static String path = Const.DATA_PATH + "at_";

    private static Meta[][] metas = new Meta[threadNum][readNum];

    public static void main(String[] args) {
//        for (int z = 0; z < 8; z++) {
//            try {
//                File file = new File(path);
//                if (!file.exists()) {
//                    file.getParentFile().mkdirs();
//                    file.createNewFile();
//                }
//                FileChannel fileChannel = FileChannel.open(Paths.get(path), StandardOpenOption.WRITE, StandardOpenOption.READ);
//                for (int z = 0; z < 8; z++) {
//                    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Integer.MAX_VALUE);
//                    for (int i = 0; i < Integer.MAX_VALUE / 8; i++) {
//                        byteBuffer.putLong(i);
//                    }
//                    byteBuffer.flip();
//                    fileChannel.write(byteBuffer);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }

//
//
//        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1000);
//        byteBuffer.putInt(6);
//        byteBuffer.slice().putInt(5);

        for (int i = 0; i < threadNum; i++) {
            for (int j = 0; j < readNum; j++) {
                int length = ThreadLocalRandom.current().nextInt(100000);
                length = (4 * 1024) * (length / (4 * 1024));
                if (length == 0) {
                    length = 4 * 1024;
                }
                metas[i][j] =
                        new Meta(
                                ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE * 2L - 100000),
                                length
                        );
            }
        }

        for (int i = 0; i < 5; i++) {
//            measureTime("fileChannel", FileReadTester::fileChannel);
//            measureTime("asyncfileChannel", FileReadTester::asyncFileChannel);
            measureTime("mappedByteBuffer", FileReadTester::mappedByteBuffer);
            System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
            try {
                Thread.sleep(1000);
            } catch (Exception e) {

            }
        }

        System.out.println("-----------------------------------------------------------");
    }

    private static void measureTime(String name, Function<Integer, List<String>> fn) {
        System.out.println("-----------------------------------------------------------");
        System.out.println("run: " + name);
        long startTime = System.nanoTime();
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            final int k = i;
            Thread t = new Thread(() -> {
                fn.apply(k);
            });
            t.start();
            threads[i] = t;
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (Exception e) {
                //
            }
        }

        long estimatedTime = System.nanoTime() - startTime;
        System.out.println("estimatedTime: " + estimatedTime / 1_000_000_000.);
    }

    private static List<String> fileChannel(Integer no) {
        try {
            FileChannel fileChannel = FileChannel.open(Paths.get(path), StandardOpenOption.WRITE, StandardOpenOption.READ);
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(100000);
            for (Meta meta : metas[no]) {
                fileChannel.position(meta.start);
                byteBuffer.limit(meta.length);
                byteBuffer.position(0);
                fileChannel.read(byteBuffer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return EMPTY_LIST;
    }

    private static List<String> asyncFileChannel(Integer no) {
        try {
            AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(Paths.get(path), StandardOpenOption.WRITE, StandardOpenOption.READ);
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(100000);
            for (Meta meta : metas[no]) {
                byteBuffer.limit(meta.length);
                byteBuffer.position(0);
                fileChannel.read(byteBuffer, meta.start).get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return EMPTY_LIST;
    }

    private static List<String> mappedByteBuffer(Integer no) {
        try {
            FileChannel fileChannel = FileChannel.open(Paths.get(path), StandardOpenOption.WRITE, StandardOpenOption.READ);
            ByteBuffer byteBuffer1 =
                    fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Integer.MAX_VALUE);
            ByteBuffer byteBuffer2 =
                    fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Integer.MAX_VALUE);
            for (Meta meta : metas[no]) {
                if (meta.start > Integer.MAX_VALUE) {
                    byteBuffer2.position((int)(meta.start - Integer.MAX_VALUE));
                    byteBuffer2.get(new byte[meta.length]);
                } else {
                    byteBuffer1.position((int)meta.start);
                    byte[] bytes = new byte[meta.length];
                    int a = (int)(Integer.MAX_VALUE - meta.start);
                    if (a < meta.length) {
                        byteBuffer1.get(bytes, 0, a);
                        byteBuffer2.position(0);
                        byteBuffer2.get(bytes, a, meta.length - a);
                    } else {
                        byteBuffer1.get(bytes);
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return EMPTY_LIST;
    }

}
