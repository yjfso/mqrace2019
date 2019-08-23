package io.openmessaging.util;

/**
 * @author yinjianfeng
 * @date 2019/8/21
 */
public class DichotomicUtil {

    public static int findLeft(byte[] data, int dst) {
        int left = 0;
        int right = data.length - 1;
        if (dst >= ByteUtil.unsignedByte(data[right])) {
            return right;
        }
        int best = left;
        while (true) {
            if (right - left < 2) {
                break;
            }
            int half = left + ((right - left) >> 1);
            int actVal = ByteUtil.unsignedByte(data[half]);
            if (actVal == dst) {
                best = half;
                break;
            }
            if (actVal > dst) {
                right = half;
            } else {
                best = left;
                left = half;
            }
        }
        //相等值向右探测
        int val = data[best];
        while (best < data.length - 1 && val == data[best + 1]) {
            val = data[++best];
        }
        return best;
    }

    public static int findRight(byte[] data, int dst) {
        int left = 0;
        int right = data.length - 1;
        int best = right;
        while (true) {
            if (right - left < 2) {
                break;
            }
            int half = left + ((right - left) >> 1);
            int actVal = ByteUtil.unsignedByte(data[half]);
            if (actVal == dst) {
                best = half;
                break;
            }
            if (actVal > dst) {
                best = right;
                right = half;
            } else {
                left = half;
            }
        }
        //相等值向左探测
        int val = data[best];
        while (best > 0 && val == data[best - 1]) {
            val = data[--best];
        }
        return best;
    }
}
