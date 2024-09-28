package cn.hedeoer.chapter01.physicalpartitioning;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 算子最大并行度测试
 */
public class MaxParallelism {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.setProperty("HADOOP_USER_NAME", "atguigu");


        // checkpoint设置
        env.enableCheckpointing(1000);
        // 其他可选配置如下：
        // 设置语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置执行Checkpoint操作时的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置最大并发执行的检查点的数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // only two consecutive checkpoint failures are tolerated
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        // enables the experimental unaligned checkpoints
//        env.getCheckpointConfig().enableUnalignedCheckpoints();
        // 将检查点持久化到外部存储
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // sets the checkpoint storage where checkpoint snapshots will be written
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink_tolerance");

//        env.setMaxParallelism(512);
        env.setMaxParallelism(520);
        env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value, 1);
                    }
                })
                .keyBy(tup -> tup.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(
                            Tuple2<String, Integer> value1,
                            Tuple2<String, Integer> value2) throws Exception {

                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();


        env.execute();
    }
}
