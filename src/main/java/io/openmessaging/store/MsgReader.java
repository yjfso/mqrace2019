package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.common.Const;
import io.openmessaging.index.DichotomicIndex;
import io.openmessaging.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yinjianfeng
 * @date 2019/8/5
 */
public class MsgReader {

    private Vfs.VfsEnum atFile = Vfs.VfsEnum.at;

    private Vfs.VfsEnum bodyFile = Vfs.VfsEnum.body;

    private DichotomicIndex index;

    public MsgReader(DichotomicIndex index){
        this.index = index;
    }

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        long minNo = index.getLeft(tMin);
        long maxNo = index.getRight(tMax);

        byte[] result = atFile.read(minNo << 4, (int)(maxNo - minNo) << 4);
        List<Integer> nos = new ArrayList<>();
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < result.length; i += 16) {

            long t = ByteUtil.bytes2long(result, i + 8);
            if (t > tMax) {
                break;
            }
            if (t < tMin) {
                continue;
            }
            long a = ByteUtil.bytes2long(result, i);
            if (a < aMin || a > aMax) {
                continue;
            }
            Message message = new Message(a, t, null);
            messages.add(message);
            nos.add(i >> 4);
        }
        byte[] bodies = bodyFile.read(minNo * Const.BODY_SIZE, (int) (maxNo - minNo) * Const.BODY_SIZE);
        for (int i = 0; i < messages.size(); i++) {
            byte[] body = new byte[Const.BODY_SIZE];
            System.arraycopy(bodies, nos.get(i) * Const.BODY_SIZE, body, 0,Const.BODY_SIZE);
            messages.get(i).setBody(body);

        }

        return messages;
    }

    public long getAvg(long aMin, long aMax, long tMin, long tMax) {
        long minNo = index.getLeft(tMin);
        long maxNo = index.getRight(tMax);

        byte[] result = atFile.read(minNo << 4, (int)(maxNo - minNo) << 4);
        long ta = 0;
        int j = 0;
        for (int i = 0; i < result.length; i += 16) {

            long t = ByteUtil.bytes2long(result, i + 8);
            if (t > tMax) {
                break;
            }
            if (t < tMin) {
                continue;
            }
            long a = ByteUtil.bytes2long(result, i);
            if (a < aMin || a > aMax) {
                continue;
            }
            ta += a;
            j++;
        }
        if (j == 0) {
            return 0;
        }
        return ta/j;
    }

}
