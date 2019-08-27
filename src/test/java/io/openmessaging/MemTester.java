package io.openmessaging;

import io.openmessaging.util.UnsafeHolder;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * @author yinjianfeng
 * @date 2019/8/27
 */
public class MemTester {

    private final static int size = 100000000;
    private static byte[] bytes = new byte[size];
    private static ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    private static ByteBuffer directByteBuffer = ByteBuffer.allocateDirect(size);

    public static void main(String[] args) {

        measureTime("byte", MemTester::testByte);
        measureTime("bytebuffer", MemTester::testByteBuffer);
        measureTime("directByteBuffer", MemTester::testDirectByteBuffer);

        System.out.println("======================");
        measureTime("byte", MemTester::testByte);
        measureTime("bytebuffer", MemTester::testByteBuffer);
        measureTime("directByteBuffer", MemTester::testDirectByteBuffer);
    }

    private static void measureTime(String name, Function<Integer, Byte> fn) {
        System.out.println("-----------------------------------------------------------");
        System.out.println("run: " + name);
        long startTime = System.nanoTime();
        for (int i = 0; i < size; i++) {
            fn.apply(i);
        }
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println("estimatedTime: " + estimatedTime / 1_000_000_000.);
    }

    public static byte testByte(int pos) {
        return bytes[pos];
    }

    public static byte testByteBuffer(int pos) {
        byteBuffer.position(pos);
        return byteBuffer.get();
    }

    public static byte testDirectByteBuffer(int pos) {
        directByteBuffer.position(pos);
        return directByteBuffer.get();
    }
}
