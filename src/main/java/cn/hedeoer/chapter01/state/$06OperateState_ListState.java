package cn.hedeoer.chapter01.state;

/**
 * operate state 和 keyed state的区别：
 * 1.作用范围： operate state 是算子实例的，keyed state 是针对每个key的，只有相同的key的流记录才能访问同一个keyState
 * 2.状态的恢复和分发：operate state恢复到之前的算子实例状态；keyed state恢复到之前的key的状态
 *
 * Operate State之ListState使用，注意区别与UnionListState
 *
 */
public class $06OperateState_ListState {
    public static void main(String[] args) {


    }
}
