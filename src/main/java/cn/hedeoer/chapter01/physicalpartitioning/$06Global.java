package cn.hedeoer.chapter01.physicalpartitioning;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 将上游的所有数据分配到下游第一个子任务中
public class $06Global {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        env.setParallelism(2);

        source.global().print();

        env.execute();
    }
}
