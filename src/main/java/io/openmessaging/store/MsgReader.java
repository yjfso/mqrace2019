package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.common.Const;
import io.openmessaging.index.TIndex;
import io.openmessaging.util.ByteUtil;
import io.openmessaging.util.DichotomicUtil;

import java.util.LinkedList;
import java.util.List;

import static io.openmessaging.common.Const.T_INTERVAL_BIT;

/**
 * @author yinjianfeng
 * @date 2019/8/5
 */
public class MsgReader {

    private Vfs.VfsEnum atFile = Vfs.VfsEnum.at;

    private Vfs.VfsEnum bodyFile = Vfs.VfsEnum.body;

    private TIndex index;

    public MsgReader(TIndex index){
        this.index = index;
    }

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        int startPile, minNo, pointer, length;
        StringBuffer stringBuffer = new StringBuffer(String.valueOf(tMin)).append(",").append(String.valueOf(tMax));

        {
            //tMin
            long pile = tMin >> T_INTERVAL_BIT;
            startPile = (int) (pile - index.startPile);
            if (startPile <= 0) {
                //tMin 小于最小值
                startPile = 0;
                pointer = 0;
            } else {
                pointer = DichotomicUtil.findRight(index.segments.get(startPile), (int) (tMin - (pile << T_INTERVAL_BIT)));
            }
            minNo = index.pileIndexes.get(startPile) + pointer;
        }

        {
            //tMax
            long pile = tMax >> T_INTERVAL_BIT;
            int endPile = (int) (pile - index.startPile);
            int endPointer;
            if (endPile >= index.pileIndexes.getPos() - 1) {
                //tMax 大于最大值
                endPile = index.pileIndexes.getPos() - 1;
                endPointer = index.segments.get(endPile).length - 1;
            } else  {
                endPointer = DichotomicUtil.findLeft(index.segments.get(endPile), (int) (tMax - (pile << T_INTERVAL_BIT)));
            }
            length = 1 + index.pileIndexes.get(endPile) + endPointer - minNo;
        }
        stringBuffer.append("minNo:").append(minNo).append(",").append("startPile")
                .append(startPile).append("pointer:").append(pointer)
                .append(",length:").append(length);
        byte[] as = atFile.read(minNo << 3,  length << 3);
        byte[] bodies = bodyFile.read(minNo * Const.BODY_SIZE,  length * Const.BODY_SIZE);
        List<Message> messages = new LinkedList<>();
        byte[] tmp = index.segments.get(startPile);

        for (int i = 0; i < length; i ++) {
            if (tmp.length == pointer) {
                pointer = 0;
                do {
                    tmp = index.segments.get(startPile ++);
                } while (tmp == null);
            }
            if (i == 0) {
                long t = ((startPile + index.startPile) << T_INTERVAL_BIT) + ByteUtil.unsignedByte(tmp[pointer]);
                if (t != tMin){
                    stringBuffer.append(":").append(t);
                }
            }
            if (i == length-1) {
                long t = ((startPile + index.startPile) << T_INTERVAL_BIT) + ByteUtil.unsignedByte(tmp[pointer]);
                if (t != tMax){
                    stringBuffer.append(",").append(t);
                }
            }
            long a = ByteUtil.bytes2long(as, i << 3);
            if (a < aMin || a > aMax) {
//                stringBuffer.append("[").append(a).append("]");
                pointer ++;
                continue;
            }
            byte[] body = new byte[Const.BODY_SIZE];
            System.arraycopy(bodies, i * Const.BODY_SIZE, body, 0, Const.BODY_SIZE);
            Message message = new Message(
                    a,
                    ((startPile + index.startPile) << T_INTERVAL_BIT) + ByteUtil.unsignedByte(tmp[pointer ++]),
                    body);
            messages.add(message);
        }
        System.out.println(stringBuffer);
        return messages;
    }

    public long getAvg(long aMin, long aMax, long tMin, long tMax) {
        long pile = tMin >> T_INTERVAL_BIT;
        int remainder = (int) (tMin - (pile << T_INTERVAL_BIT));
        int startPile = (int) (pile - index.startPile);
        int pointer;
        if (startPile < 0) {
            startPile = 0;
            pointer = 0;
        } else {
            pointer = DichotomicUtil.findRight(index.segments.get(startPile), remainder);
        }
        int minNo = index.pileIndexes.get(startPile) + pointer;

        pile = tMax >> T_INTERVAL_BIT;
        remainder = (int) (tMax - (pile << T_INTERVAL_BIT));
        int endPile = (int) (pile - index.startPile);
        int endPointer;
        if (endPile >= index.pileIndexes.getPos()) {
            endPile = index.pileIndexes.getPos() - 1;
            endPointer = index.segments.get(endPile).length - 1;
        } else  {
            endPointer = DichotomicUtil.findLeft(index.segments.get(endPile), remainder);
        }
        int maxNo = index.pileIndexes.get(endPile) + endPointer;

        long ta = 0;
        int j = 0;
        int length = 1 + maxNo - minNo;
        if (length < 0) {
            System.out.println(length);
        }
        byte[] as = atFile.read(minNo << 3,  length << 3);
        for (int i = 0; i < length; i ++) {
            long a = ByteUtil.bytes2long(as, i << 3);
            if (a < aMin || a > aMax) {
                pointer ++;
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
