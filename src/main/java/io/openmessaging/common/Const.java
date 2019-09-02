package io.openmessaging.common;

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

    public final static int MAX_PUT_SIZE = 500 * 1024;

    public final static int T_INTERVAL_BIT = 8;

    public final static int T_INTERVAL_NUM = 1 << 8 - 1;

    public final static long AT_SPLIT = -1;

    public final static int MAX_GET_MSG_NUM = 50_0000;

}
