package io.openmessaging;

import sun.misc.Unsafe;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yinjianfeng
 * @date 2019/8/3
 */
public class AvgTester {

    public final static Unsafe UNSAFE = getUnsafe();

    private static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        System.out.println(5 >> 2 << 2);
        System.out.println(6 >> 2 << 2);
//        System.out.println(3 >> 1);
//        System.out.println(1 << 2);
//        System.out.println(1 << 3);
//        System.out.println(8 >> 4);
//        System.out.println("----------");
//        ByteBuffer.allocate(99999);
//
//        ByteBuffer.allocateDirect(Integer.MAX_VALUE);
//        List<Object> objectList = new ArrayList<>();
//        for (int i = 0; i < 999999999; i++) {
//            main(args);
//        }
//
//        try{
//            long st = UNSAFE.allocateMemory(2L * 1024 * 1024 * 1024);
//            long st1 = st;
//            for (int i = 0; i < 2L * 1024 * 1024 * 1024 / 4; i++) {
//                UNSAFE.putInt(st1, i);
//                st1 += 4;
//            }
//            st1 = st;
//            for (int i = 0; i < 2L * 1024 * 1024 * 1024 / 4; i++) {
//                UNSAFE.getInt(st1);
//                st1 += 4;
//            }
//
//
////            SkipIndex.UNSAFE.allocateMemory(2L * 1024 * 1024);
//            File file = new File("./test");
////            if (!file.exists()) {
////                file.createNewFile();
////            }
////            Path path = file.toPath();
//            MappedByteBuffer buffer = new RandomAccessFile(file, "rw").getChannel()
////            MappedByteBuffer mappedByteBuffer = FileChannelImpl.open(path)
//                    .map(FileChannel.MapMode.READ_WRITE, 0, 999999999);
//            for (int i = 0; i < 999999999; i++) {
//                buffer.put((byte)3);
//            }
//            buffer.position(0);
//            for (int i = 0; i < 999999990; i++) {
//                buffer.get();
//            }
//            Thread.sleep(999999999);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        long t = 10000000000l;
        for (long i = 0; i < t; i++) {
            int a = 1 - 1;
            boolean b = 1 == 1;

//            long k = i % 8;
        }
        long s = System.currentTimeMillis();
        long e;
        for (long i = 0; i < t; i++) {
            boolean a = 1 == 1;

//            long k = i % 8;
        }
        e = System.currentTimeMillis();
        System.out.println(e - s);
        System.out.println("==============");
        s = System.currentTimeMillis();
        for (long i = 0; i < t; i++) {
            int a = 1 - 1;

//            long k = i & 0b111;
        }
        e = System.currentTimeMillis();
        System.out.println(e - s);

        s = System.currentTimeMillis();
        for (long i = 0; i < t; i++) {
            Math.random();
        }
        e = System.currentTimeMillis();
        System.out.println(e - s);
    }
}
