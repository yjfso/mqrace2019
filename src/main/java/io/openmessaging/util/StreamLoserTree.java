package io.openmessaging.util;

import java.util.List;

/**
 * @author yinjianfeng
 * @date 2019/8/15
 */
public class StreamLoserTree<T extends StreamTreeNode<T, K>, K> {

    private byte[] tree;

    private byte size;

    private List<T> leaves;

    public StreamLoserTree(List<T> leaves) {
        this.leaves = leaves;
        this.size = (byte) leaves.size();
        this.tree = new byte[size];
        rebuild();
    }

    private void remove(int index) {
        leaves.remove(index);
        if (size-- > 0) {
            this.tree = new byte[size];
            rebuild();
        }
    }

    private void rebuild() {
        for (byte i = 0; i < size; ++i) {
            tree[i] = -1;
        }
        for (byte i = (byte)(size - 1); i >= 0; --i) {
            adjust(i);
        }
    }

    public K askWinner() {
        if (size == 0) {
            System.out.println("threadMessages size = 0, write done");
            return null;
        }
        T t = leaves.get(tree[0]);
        K k = t.pop();
        if (t.isEmpty()) {
            System.out.println(t + " is empty, will remove");
            remove(tree[0]);
        } else {
            adjust(tree[0]);
        }
        return k;
    }

    private void adjust(byte winner) {
        int node = (winner + size) / 2;

        while (node > 0) {
            if (winner == -1) {
                break;
            }
            if (tree[node] == -1) {
                byte tmp = winner;
                winner = tree[node];
                tree[node] = tmp;
            } else {
                T t1 = leaves.get(winner);
                T t2 = leaves.get(tree[node]);
                if (t2.lessAndEqual(t1)) {
                    byte tmp = winner;
                    winner = tree[node];
                    tree[node] = tmp;
                }
            }

            node >>= 1;
        }
        tree[0] = winner;
    }
}
