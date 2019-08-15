package io.openmessaging.util;

/**
 * @author yinjianfeng
 * @date 2019/8/15
 */
public interface StreamTreeNode<T, K> extends Comparable<T> {

    boolean isEmpty();

    K pop();
}
