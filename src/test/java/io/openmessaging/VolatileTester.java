package io.openmessaging;

import io.openmessaging.util.Ring;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yinjianfeng
 * @date 2019/8/25
 */
public class VolatileTester {

    private static int[] code = new int[1];

    private static volatile int vol = 2;

    private final static Random RANDOM = new Random(1);

    public static void main(String[] args) {
        Ring<Object> ring = new Ring<Object>(new Object[30]);
        ring.fill(Object::new);
        int time = 9999999;
        System.out.println("start=====");
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        new Thread(
                () -> {
                    for (int i = 0; i < time; i++) {
                        try {
                            Thread.sleep(10);
                        } catch (Exception e) {

                        }
                        Object object = ring.popWait();
                        executorService.submit(
                                () -> {
                                    try {
                                        Thread.sleep(RANDOM.nextInt(21));
                                    } catch (Exception e) {

                                    }
                                    ring.add1(object);
                                }
                        );
                    }
                    try {
                        Thread.sleep(1000);
                        code[0] = 2;
                    } catch (Exception e) {

                    }
                }
        ).start();
//        new Thread(() -> {
//            for (int i = 0; i < time; i++) {
//                ring.add1();
//            }
//            int i = vol;
//            while (code[0] != 2) {
////                System.out.println(code);
//                try {
////                    Thread.sleep(1);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            System.out.println("done");
//        }).start();
    }
}
