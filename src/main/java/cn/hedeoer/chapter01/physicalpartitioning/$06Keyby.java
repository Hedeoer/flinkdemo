package cn.hedeoer.chapter01.physicalpartitioning;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 根据指定的键（key）对数据进行分区，相同键的记录被发送到同一个下游任务
public class $06Keyby {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        env.setParallelism(10);

        source.keyBy(data -> data)
                        .print();

        env.execute();
    }
}
