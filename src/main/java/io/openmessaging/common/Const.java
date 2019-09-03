package io.openmessaging.common;

import io.openmessaging.util.UnsafeHolder;

/**
 * @author yinjianfeng
 * @date 2019/8/2
 */
public class Const {

    public final static String DATA_PATH = "/alidata1/race2019/data/";

    public final static int MSG_NUM = 2100000000;

    public final static int INDEX_INTERVAL = 511;

    public final static int BODY_SIZE = 34;

    public final static int A_SIZE = 8;

    public final static int WRITE_ASYNC_NUM = 300;

    public final static int MAX_DUMP_SIZE = 64 * 1024;

    public final static int MAX_PUT_SIZE = 1000 * 1024;

    public final static int T_INTERVAL_BIT = 8;

    public final static int T_INTERVAL_NUM = (1 << 8) - 1;

    public final static int T_SIZE = 600 << 20;

    public final static int MAX_GET_MSG_NUM = 50_0000;

    public final static int THREAD_NUM = 4;

    public final static long ARRAY_BASE_OFFSET = UnsafeHolder.UNSAFE.arrayBaseOffset(byte[].class);

    public final static long LONG_ARRAY_BASE_OFFSET = UnsafeHolder.UNSAFE.arrayBaseOffset(long[].class);

    public final static long BUFFER_ADDRESS_OFFSET = UnsafeHolder.bufferAddressOffset();
}
