package io.openmessaging.store;

/**
 * @author yinjianfeng
 * @date 2019/8/29
 */
public class IndexIterator {

    long t;

    int baseCursor;

    int cursor;

    private long startNo;

    private int length;

    private TBits tBits;

    public IndexIterator (TBits tBits) {
        this.tBits = tBits;
    }

    public void initBase(long baseT, int baseNo) {
        this.t = baseT;
        this.startNo = baseNo;
    }

    public void initIncrement(int incrementT, int incrementNo) {
        this.t += incrementT;
        this.startNo += incrementNo;
    }

    public void initCursor(int baseCursor, int cursor) {
        this.baseCursor = baseCursor;
        this.cursor = cursor;
    }

    public void setEndNo(long endNo) {
        length = (int) (endNo - startNo);
        if (length < 0) {
            System.out.println("===");
        }
    }

    public long getStartNo() {
        return startNo;
    }

    public int getLength() {
        return length;
    }

    public long nextT() {
        try {
            tBits.nextT(this);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return t;
    }

}
