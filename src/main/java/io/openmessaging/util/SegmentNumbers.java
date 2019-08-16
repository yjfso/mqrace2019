package io.openmessaging.util;

import io.openmessaging.common.Const;

/**
 * @author yinjianfeng
 * @date 2019/8/16
 */
public class SegmentNumbers {

    private final static byte[][] SEGMENTS = new byte[Const.MSG_NUM / Const.BYTE_MAX_CAP][];

    private final static int[] PILE = new int[Const.MSG_NUM];

    public void put() {
    }
}
