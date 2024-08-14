package cn.hedeoer.chapter01.physicalpartitioning;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 全局打乱数据顺序，适用于全局排序等场景
public class $01Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        env.setParallelism(2);

        source.shuffle().print();

        env.execute();
    }
}
