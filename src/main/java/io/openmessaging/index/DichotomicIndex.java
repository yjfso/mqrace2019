package io.openmessaging.index;

import io.openmessaging.common.Const;
import io.openmessaging.util.ByteUtil;

/**
 * @author yinjianfeng
 * @date 2019/8/9
 */
public class DichotomicIndex {

    class Meta {

        private byte[] bytes;

        private int pos;

        Meta(int size) {
            bytes = new byte[size];
        }

        void putInt(int val) {
            ByteUtil.int2bytes(val, bytes, pos);
            pos += 4;
        }

        void putLong(long val) {
            ByteUtil.long2bytes(val, bytes, pos);
            pos += 8;
        }

        long getLong(int pos) {
            return ByteUtil.bytes2long(bytes, pos);
        }

        int getInt(int pos) {
            return ByteUtil.bytes2int(bytes, pos);
        }
    }

    private Meta tMeta = new Meta(8 * (1 + Const.MSG_NUM / Const.INDEX_INTERVAL));

    private Meta noMeta = new Meta(4 * (1 + Const.MSG_NUM / Const.INDEX_INTERVAL));

    private int size;

    /**
     *
     * @param t t
     * @param no 所在序列
     */
    public void put(long t, int no) {
        tMeta.putLong(t);
        noMeta.putInt(no);
        size ++;
    }

    public int getLeft(long t) {
        int left = 0;
        int right = size - 1;
        int best = left;
        while (true) {
            if (right - left < 2) {
                break;
            }
            int half = left + ((right - left) >> 1);
            long dst = tMeta.getLong(half << 3);
            if (dst == t) {
                break;
            }
            if (dst > t) {
                right = half;
            } else {
                best = left;
                left = half;
            }
        }

        return noMeta.getInt(best << 2);
    }

    public int getRight(long t) {
        int left = 0;
        int right = size - 1;
        int best = right;
        while (true) {
            if (right - left < 2) {
                break;
            }
            int half = left + ((right - left) >> 1);
            long dst = tMeta.getLong(half << 3);
            if (dst == t) {
                break;
            }
            if (dst > t) {
                best = right;
                right = half;
            } else {
                left = half;
            }
        }

        return noMeta.getInt(best << 2);
    }
}