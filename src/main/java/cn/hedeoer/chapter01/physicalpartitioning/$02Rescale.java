package cn.hedeoer.chapter01.physicalpartitioning;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 一种优化的重平衡分区，数据按照上下游任务的并行度比例进行分配，使得数据尽量保持本地性(同一个taskmanager)。减少了不同taskmanager间的网络通信传输重新分配数据
// 区别与reblance
public class $02Rescale {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        env.setParallelism(2);

        source.rescale().print();

        env.execute();
    }
}
