package cn.hedeoer.taskconfig;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamConfig {
    public static void setCheckPoint(StreamExecutionEnvironment env){
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
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 如果有更近的保存点时，是否将作业回退到该检查点
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // sets the checkpoint storage where checkpoint snapshots will be written
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink_tolerance");

        // 并行度设置，如何不配置，线上环境，默认任务的并行度为1，即flink-conf.yaml中parallelism.default为1
//        env.setParallelism(4);
    }
}
