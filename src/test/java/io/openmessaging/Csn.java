package io.openmessaging;

/**
 * @author yinjianfeng
 * @date 2019/9/1
 */
public class Csn {

    public static void main(String[] args) {
        int[] ints = new int[] {Byte.MAX_VALUE, Byte.MAX_VALUE,Byte.MAX_VALUE,Byte.MAX_VALUE,
                Byte.MAX_VALUE,Byte.MAX_VALUE,Byte.MAX_VALUE,Byte.MAX_VALUE,1};

        long a = 0;
        int max = 0;
        for (int anInt : ints) {
            max = Math.max(anInt, max);
        }

        for (int anInt : ints) {
            a = a * max + anInt;
        }
        System.out.println(a);
    }
}
