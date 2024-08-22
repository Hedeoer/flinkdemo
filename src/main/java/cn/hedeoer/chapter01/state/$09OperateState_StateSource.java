package cn.hedeoer.chapter01.state;

// operate state 常常用于sink或者source的算子中，source或者sink中常常没有key的要求，状态的划分常常没有要求到按照key进行划分的程度
// 那么在状态的恢复和存储时使用算子状态即可（operate state）


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 算子状态的失败恢复的常见两种机制：
 * 1. Even-split redistribution：每个子任务实例重新“均匀”分配状态，比如liststate
 * 2. Union redistribution：每个子任务实例都获取一份相同的状态，比如unionliststate
 * 此处演示liststate在 source 中的使用，主要实现 CheckpointedFunction 接口
 */
public class $09OperateState_StateSource {
    /**
     * 创建一个source,该source的功能为产生一系列递增的长整形数
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 此处方便测试，生产环境合理设置
        env.setParallelism(1);

        env.addSource(new CounterSource())
                        .print();


        env.execute();

    }

    public static class CounterSource
            extends RichParallelSourceFunction<Long>
            implements CheckpointedFunction {

        /**  current offset for exactly once semantics */
        private Long offset = 0L;

        /** flag for job cancellation */
        private volatile boolean isRunning = true;

        /** Our state object. */
        private ListState<Long> state;

        @Override
        public void run(SourceContext<Long> ctx) {
            final Object lock = ctx.getCheckpointLock();

            while (isRunning) {
                // output and state update are atomic
                synchronized (lock) {
                    ctx.collect(offset);
                    offset += 1;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        /**
         * source算子状态的恢复执行 initializeState（）
         * @param context the context for initializing the operator
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                    "state",
                    LongSerializer.INSTANCE));

            // restore any state that we might already have to our fields, initialize state
            // is also called in case of restore.
            for (Long l : state.get()) {
                offset = l;
            }
        }

        /**
         * source算子状态的快照执行 snapshotState（）
         * @param context the context for drawing a snapshot of the operator
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(offset);
        }
    }
}
