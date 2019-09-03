package io.openmessaging.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.openmessaging.common.Const.THREAD_NUM;

public class Common {

    public final static ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(THREAD_NUM);
}
