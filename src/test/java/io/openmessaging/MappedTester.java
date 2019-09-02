package io.openmessaging;

import io.openmessaging.store.Vfs;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author yinjianfeng
 * @date 2019/8/26
 */
public class MappedTester {

    public static void main(String[] args) {

        for (int i = 0; i < 10; i++) {
            Vfs.VfsEnum.at.vfs.setWritePos(Integer.MAX_VALUE * 2L);
            new Thread(
                    () -> {
                        while (true) {
                            Vfs.VfsEnum.at.read(ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE * 2L - 100000),
                                    ThreadLocalRandom.current().nextInt(100000));
                        }
                    }
            ).start();
        }
    }
}
