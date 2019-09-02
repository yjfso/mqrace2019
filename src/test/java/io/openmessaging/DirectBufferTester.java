package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

/**
 * @author yinjianfeng
 * @date 2019/8/31
 */
public class DirectBufferTester {

    private final static int length = 1000000000;

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            measureTime("jvm", DirectBufferTester::jvm);
            measureTime("direct", DirectBufferTester::direct);
            System.out.println("\n\n\n");
        }

    }

    private static void measureTime(String name, Function<Integer, List<String>> fn) {
        System.out.println("-----------------------------------------------------------");
        System.out.println("run: " + name);
        long startTime = System.nanoTime();
        fn.apply(1);
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println("estimatedTime: " + estimatedTime / 1_000_000_000.);
    }

    public static List<String> direct (int j) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(length);
        for (int i = 0; i < length; i++) {
            byteBuffer.put((byte) i);
        }
        return null;
    }

    public static List<String> jvm (int j) {
        byte[] bytes = new byte[length];
//        ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) i;
//            byteBuffer.put((byte) i);
        }
//        byteBuffer.flip();
//        ByteBuffer byteBuffer1 = ByteBuffer.allocateDirect(length);
//        byteBuffer1.put(byteBuffer);
        return null;
    }

}
