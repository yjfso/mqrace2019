package io.openmessaging.util;

import io.openmessaging.common.Const;

/**
 * @author yinjianfeng
 * @date 2019/8/24
 */
public class ByteObjectPool {

    class Node {

        byte[] node = new byte[Const.BODY_SIZE];

        Node next;
    }

    private Node root = new Node();

    private Node now = root;

    public byte[] borrowObject() {
        if (now.next == null) {
            now.next = new Node();
        }
        try {
            return now.node;
        } finally {
            now = now.next;
        }
    }

    public void returnAll() {
        now = root;
    }
}
