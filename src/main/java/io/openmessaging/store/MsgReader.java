package io.openmessaging.store;

import io.openmessaging.Message;
import io.openmessaging.common.Const;
import io.openmessaging.index.TIndex;
import io.openmessaging.util.ByteObjectPool;
import io.openmessaging.util.ByteUtil;
import io.openmessaging.util.DichotomicUtil;

import java.nio.ByteBuffer;
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

    private ThreadLocal<ByteObjectPool> bodyByte = ThreadLocal.withInitial(ByteObjectPool::new);

    public MsgReader(TIndex index){
        this.index = index;
    }

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        int startPile, pointer, length;
        long minNo;
        {
            //tMin
            long pile = tMin >> T_INTERVAL_BIT;
            startPile = (int) (pile - index.startPile);
            if (startPile <= 0) {
                //tMin 小于最小值
                startPile = 0;
                pointer = 0;
            } else {
                pointer = DichotomicUtil.findGte(index.segments.get(startPile), (int) (tMin - (pile << T_INTERVAL_BIT)));
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
                endPointer = DichotomicUtil.findLte(index.segments.get(endPile), (int) (tMax - (pile << T_INTERVAL_BIT)));
            }
            length = 1 + index.pileIndexes.get(endPile) + endPointer - (int)minNo;
        }
        byte[] as = atFile.read(minNo << 3,  length << 3);
        byte[] bodies = bodyFile.read(minNo * Const.BODY_SIZE,  length * Const.BODY_SIZE);
        List<Message> messages = new LinkedList<>();
        ByteObjectPool byteObjectPool = bodyByte.get();
        byte[] tmp = index.segments.get(startPile);

        for (int i = 0; i < length; i ++) {
            if (tmp.length == pointer) {
                pointer = 0;
                do {
                    tmp = index.segments.get(++startPile);
                } while (tmp == null);
            }

            long a = ByteUtil.bytes2long(as, i << 3);
            if (a < aMin || a > aMax) {
                pointer ++;
                continue;
            }
            byte[] body = byteObjectPool.borrowObject();
            System.arraycopy(bodies, i * Const.BODY_SIZE, body, 0, Const.BODY_SIZE);
            Message message = new Message(
                    a,
                    ((startPile + index.startPile) << T_INTERVAL_BIT) + ByteUtil.unsignedByte(tmp[pointer ++]),
                    body);
            messages.add(message);
        }
        byteObjectPool.returnAll();
        return messages;
    }

    public void getMessageDone() {
        bodyFile = null;
        bodyByte = null;
    }

    public long getAvg(long aMin, long aMax, long tMin, long tMax) {
        int startPile, pointer, length;
        long minNo;

        {
            //tMin
            long pile = tMin >> T_INTERVAL_BIT;
            startPile = (int) (pile - index.startPile);
            if (startPile <= 0) {
                //tMin 小于最小值
                startPile = 0;
                pointer = 0;
            } else {
                pointer = DichotomicUtil.findGte(index.segments.get(startPile), (int) (tMin - (pile << T_INTERVAL_BIT)));
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
                endPointer = DichotomicUtil.findLte(index.segments.get(endPile), (int) (tMax - (pile << T_INTERVAL_BIT)));
            }
            length = 1 + index.pileIndexes.get(endPile) + endPointer - (int)minNo;
        }
        byte[] as = atFile.read(minNo << 3,  length << 3);
        long ta = 0;
        int j = 0;
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
