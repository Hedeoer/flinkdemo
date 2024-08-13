package cn.hedeoer.chapter01.state;

// operate state 常常用于sink或者source的算子中，source或者sink中常常没有key的要求，状态的划分常常没有要求到按照key进行划分的程度
// 那么在状态的恢复和存储时使用算子状态即可（operate state）

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 算子状态的失败恢复的常见两种机制：
 * 1. Even-split redistribution：每个子任务实例重新“均匀”分配状态，比如liststate
 * 2. Union redistribution：每个子任务实例都获取一份相同的状态，比如unionliststate
 * 此处演示liststate在 sink算子中的实现，主要实现CheckpointedFunction接口
 */
public class $08OperateState_BufferSink {
    /**
     * 创建一个带有缓冲的sink算子，按照元素的个数，每累计批次元素个数到达2个，就sink一次
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 此处方便测试，生产环境合理设置
        env.setParallelism(1);

        DataStream<String> sourceStream = env.fromElements("one", "two", "three", "four", "five");

        DataStreamSink<String> sinkStream
                = sourceStream.addSink(new BufferingSinkFunction<String>(2L));// 设置缓冲区大小为2


        env.execute();

    }

    public static class BufferingSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

        private Long bufferSize;

        private List<T> bufferElements;

        // 存储的状态
        private  transient ListState<T> checkpointedState;


        // buffersize 和 bufferElements 初始化
        public BufferingSinkFunction(Long bufferSize) {
            this.bufferSize = bufferSize;
            this.bufferElements = new ArrayList<>();
        }


        /**
         * 每个sink算子实例都会对应一个open方法，用于sink的初始化
         * @param parameters The configuration containing the parameters attached to the contract.
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        // BufferingSinkFunction方法关闭之前，将缓冲区中的元素sink到其他系统
        // 确保元素都sink了
        @Override
        public void close() throws Exception {
            if (!bufferElements.isEmpty()) {
                flush();
            }
        }

        /**
         * sink算子每接受到一个元素调用一次
         * @param value The input record.
         * @param context Additional context about the input record.
         * @throws Exception
         */
        @Override
        public void invoke(T value, Context context) throws Exception {
            bufferElements.add(value);
            if (bufferElements.size() == 2) {
                flush();
            }
        }

        /**
         * 达到 checkpoint 触发时间调用一次
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 每次存储新的状态前，将之前的状态清空
            checkpointedState.clear();
            // 将新的状态添加到状态中
            for (T bufferElement : bufferElements) {
                checkpointedState.add(bufferElement);
            }
        }

        /**
         * 作业启动或恢复时对状态进行初始化一次
         * @param context the context for initializing the operator
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            ListStateDescriptor<T> descriptor = new ListStateDescriptor<>("buffered-elements",  TypeInformation.of((Class<T>) Object.class));
            // 获取状态描述并赋值
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            // 判断是否需要从已有的状态恢复
            if (context.isRestored()) {
                for (T t : checkpointedState.get()) {
                    bufferElements.add(t);
                }
            }
        }


        public void flush(){
            // 此处将sink算子接受的元素输出到标准输出（控制台），生产中对应可能就是一些数据库或者文件存储

            for (T bufferElement : bufferElements) {
                System.out.println("sink out: " + bufferElement);
            }
            // sink到其他系统后，清空bufferElements
            bufferElements.clear();
        }
    }
}
