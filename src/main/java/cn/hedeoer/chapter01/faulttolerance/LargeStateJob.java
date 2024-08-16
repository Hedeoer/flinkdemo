package cn.hedeoer.chapter01.faulttolerance;

import cn.hedeoer.taskconfig.StreamConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 *HashMapStateBackend 和 RockDB的选择
 *0. 可以通过为不同的job设置默认的状态后端
 *1. HashMapStateBackend使用heap 内存（jobmanager），任务恢复速度快，但遇到大状态时，对内存要求极高，也是flink默认的配置
 *2. RockDB，反之
 * rockdb的选择时，在本地开发时，需要引入相关的依赖，
 * flink默认的状态存储为HashMapStateBackend ，存储在JVM内存中，如果任务执行时间过长，内存会耗尽
 * 此处模拟大状态，使用rockdb存储，并cancel任务后，使用checkpoint检查点恢复任务
 */
public class LargeStateJob {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // job执行环境配置
        StreamConfig.setCheckPoint(env);
        // 为当前job设置状态后端为rocksdb，并开启增量备份
        // 也可以通过flink-conf.yaml设置，#state.backend: rocksdb，#state.backend.incremental: true，则所有flink任务生效
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink_tolerance", true));

        env.fromSequence(1, Long.MAX_VALUE)
           .keyBy(value -> value % 1000)  // 模拟多个 key，每个 key 的状态会很大
           .process(new LargeStateFunction())
           .print();

        env.execute("Flink Large State Job");
    }

    public static class LargeStateFunction extends KeyedProcessFunction<Long, Long, String> {

        private ValueState<Map<Long, Long>> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Map<Long, Long>> descriptor =
                    new ValueStateDescriptor<>("largeState", Types.MAP(Types.LONG, Types.LONG));
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Long value, Context ctx, Collector<String> out) throws Exception {
            // 获取当前状态
            Map<Long, Long> currentState = state.value();
            if (currentState == null) {
                currentState = new HashMap<>();
            }

            // 模拟大状态，每个 key 存储大量数据
            for (long i = 0; i < 10000; i++) {
                currentState.put(i, value);
            }

            // 更新状态
            state.update(currentState);

            // 输出状态大小
            out.collect("Key: " + ctx.getCurrentKey() + " State Size: " + currentState.size());
        }
    }
}
