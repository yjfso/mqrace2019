package io.openmessaging;

import io.openmessaging.util.Ring;

/**
 * @author yinjianfeng
 * @date 2019/8/17
 */
public class RingTest {

    public static void main(String[] args) {
        Ring<Integer> ring = new Ring<>(new Integer[1000]);

        new Thread(
                ()-> {
                    for (int i = 0; i < Integer.MAX_VALUE; i++) {
                        ring.add(i);
                    }
                }
        ).start();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            Integer pop = ring.pop();
            while (pop == null) {
                try{
                    Thread.sleep(10);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                pop = ring.pop();
            }
            if (i != pop) {
                System.out.println("error" + i);
                System.exit(0);
            }
        }
    }
}
