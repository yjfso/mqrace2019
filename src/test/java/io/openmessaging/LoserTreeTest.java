package io.openmessaging;

import io.openmessaging.bean.ThreadMessage;
import io.openmessaging.bean.ThreadMessageManager;
import io.openmessaging.util.StreamLoserTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author yinjianfeng
 * @date 2019/8/17
 */
public class LoserTreeTest {

    public static void main(String[] args) throws Exception {
        List<ThreadMessage> leaves = new ArrayList<>();
        ThreadMessageManager threadMessageManager = new ThreadMessageManager();
        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                ThreadMessage threadMessage = new ThreadMessage(threadMessageManager);
                Random random = new Random();
                long t = (long)(random.nextDouble() * 10000000);
                for (long j = 1; j < 10000L; j++) {
                    t = t + (long)(random.nextDouble() * 10);
                    threadMessage.put(new Message(t, t, new byte[0]));
                }

            }).start();
        }
        Thread.sleep(100);
        StreamLoserTree<ThreadMessage, Message> streamLoserTree = new StreamLoserTree<>(threadMessageManager.getTM());
        long last = 0;
        StringBuffer stringBuffer = new StringBuffer();
        while (true) {
            Message message = streamLoserTree.askWinner();
            if (message == null) {
                break;
            }
            if (message.getT() < last) {
                System.out.println("============");
                System.out.println(message.getT());
                System.exit(0);
            }
            stringBuffer.append(message.getT()).append(",");
//            System.out.println(message.getT());
        }
        System.out.println(stringBuffer);
    }
}
