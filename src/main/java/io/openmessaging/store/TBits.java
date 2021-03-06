package io.openmessaging.store;

import io.openmessaging.common.Const;
import io.openmessaging.util.UnsafeHolder;

import java.nio.ByteBuffer;

import static io.openmessaging.common.Const.T_INTERVAL_NUM;
import static io.openmessaging.common.Const.T_SIZE;
import static io.openmessaging.util.UnsafeHolder.UNSAFE;

/**
 * @author yinjianfeng
 * @date 2019/8/27
 */
public class TBits {

    /**
     * tBits   每位1表示1个数(No)  每位0表示实际数据加1 (T)
     * 如：
     * 2、2、3、5
     * 011010010
     */
//    private static DynamicIntArray tByteBuffer = new DynamicIntArray(105_000_000, (byte) 15);

    private static ByteBuffer tByteBuffer = ByteBuffer.allocateDirect(T_SIZE);//.asIntBuffer();

    long tAddress = UNSAFE.getLong(tByteBuffer, Const.BUFFER_ADDRESS_OFFSET);

    private int no;

    private long nowAddress = tAddress;

    private final static byte BIT_LENGTH = 32;

    private final static byte BIT_LENGTH_MOVE = 5;

    private int val;

    private byte surplusNum = BIT_LENGTH;

    public void setBit(int length) {
        if (length >= surplusNum) {
            if (surplusNum == BIT_LENGTH) {
                val = -1;
            } else {
                val |= (1 << surplusNum) - 1;
            }
            put(val);
//            tByteBuffer.put(val);
            val = 0;

            int writeLength = length - surplusNum;
            int multiple = writeLength >>> BIT_LENGTH_MOVE;
            surplusNum = (byte) (writeLength - (multiple << BIT_LENGTH_MOVE));
            if (surplusNum == 0) {
                surplusNum = BIT_LENGTH;
            } else {
                val |= (1 << surplusNum) - 1;
                surplusNum = (byte) (BIT_LENGTH - surplusNum);
                val <<= surplusNum;
            }

            for (; multiple > 0; multiple--) {
                put(-1);
//                tByteBuffer.put(-1);
            }

        } else {
            surplusNum -= length;
            val |= (((1 << length) - 1) << surplusNum);
        }
    }

    public void clearBit(int length) {
        if (length == surplusNum) {
            put(val);
//            tByteBuffer.put(val);
            val = 0;
            surplusNum = BIT_LENGTH;
        } else if (length > surplusNum) {
//            tByteBuffer.put(val);
            put(val);
            val = 0;

            int writeLength = length - surplusNum;
            int multiple = writeLength >>> BIT_LENGTH_MOVE;
            surplusNum = (byte) (BIT_LENGTH - (writeLength - (multiple << BIT_LENGTH_MOVE)));

//            UnsafeHolder.UNSAFE.put(pos, val);
            nowAddress += (multiple << 2);
            no += multiple;
//            for (; multiple > 0; multiple--) {
//                tByteBuffer.put(0);
//            }

        } else {
            surplusNum -= length;
        }
    }

    public void initIteratorStart(int pos, int dst, IndexIterator indexIterator) {
        if (dst == 0) {
            indexIterator.initCursor(pos, BIT_LENGTH - 1);
            return;
        }
        int zeroNum = dst;

        int length = no - pos;
        for (int i = 0; i < length; i++) {
            int val = get(pos);
            int num0 = 0;
            while(val != -1) {
                val = val | (val + 1);
                num0++;
            }
            if (zeroNum <= num0) {
                val = get(pos);
                for (int j = BIT_LENGTH - 1; j >= 0; j--) {
                    if ((val & (1 << j)) == 0) {
                        if (--zeroNum == 0) {
                            //满足条件的0
                            indexIterator.initCursor(pos, j - 1);
                            indexIterator.initIncrement(dst, (i + 1) * BIT_LENGTH - j - dst);
                            return;
                        }
                    }
                }
            }
            pos ++;
            zeroNum -= num0;
        }
        throw new RuntimeException("");
    }

    public long endNo(int pos, int dst) {
        int zeroNum = dst + 1;

        int length = no - pos;
        for (int i = 0; i < length; i++) {
            int val = get(pos++);
            int num0 = 0;
            while(val != -1) {
                val = val | (val + 1);
                num0++;
            }
            if (zeroNum <= num0) {
                val = get(pos - 1);
                for (int j = BIT_LENGTH - 1; j >= 0; j--) {
                    if ((val & (1 << j)) == 0) {
                        if (--zeroNum == 0) {
                            //满足条件的0
                            return (i + 1) * BIT_LENGTH - j - dst;
                        }
                    }
                }
            }
            zeroNum -= num0;
        }
        return T_INTERVAL_NUM;
    }

    public int getPos() {
        if (surplusNum != BIT_LENGTH) {
            put(val);
//            tByteBuffer.put(val);
            val = 0;
            surplusNum = BIT_LENGTH;
        }
        return no;
    }

    private int get(int pos) {
        return UnsafeHolder.UNSAFE.getInt(tAddress + (pos << 2));
    }

    private void put(int val) {
        UnsafeHolder.UNSAFE.putLong(nowAddress, val);
        no ++;
        nowAddress += 4;
    }

    public void writeDone() {
        put(val);
//        tByteBuffer.put(val);
//        tByteBuffer.slice();
//        tByteBuffer.flip();
//        tByteBuffer = tByteBuffer.slice();
//        DirectBuffer.writeDown(tByteBuffer.capacity() * 4);
    }

    public boolean nextT(IndexIterator indexIterator) {
        int val = get(indexIterator.baseCursor);

        while (true) {
            while (indexIterator.cursor >= 0) {
                if ((val & (1 << indexIterator.cursor--)) == 0) {
                    if ((++indexIterator.t & ((1 << Const.T_INTERVAL_BIT) - 1)) == 0) {
                        //忽略tBits分段尾部对齐数据
                        break;
                    }
                } else {
                    return true;
                }
            }
            val = get(++indexIterator.baseCursor);
            indexIterator.cursor = BIT_LENGTH - 1;
        }
    }

    public static void main(String[] args) {
        TBits tBits = new TBits();
        tBits.setBit(32);
        tBits.clearBit(1);
        tBits.setBit(2);
        tBits.clearBit(1);
        tBits.setBit(3589);
        tBits.clearBit(5);
        tBits.getPos();
        tBits.initIteratorStart(0, 2, new IndexIterator(null));
//        System.out.println(tBits.getRealStartNum(0,8));
    }
}
