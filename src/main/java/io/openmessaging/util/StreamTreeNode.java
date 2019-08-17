package io.openmessaging.util;

/**
 * @author yinjianfeng
 * @date 2019/8/15
 */
public interface StreamTreeNode<T, K>{

    boolean isEmpty();

    K pop();

    boolean lessAndEqual(T o);
}
