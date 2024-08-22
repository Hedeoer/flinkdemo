package cn.hedeoer.chapter01.source;

import cn.hedeoer.common.datatypes.Event;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.jupiter.api.Test;

import java.util.List;


// 使用SourceFunction接口创建用户自己的数据源 实现 SourceFunction
// 也可以为自定义的source，实现flink的状态管理 实现
public class $03UserDefinedSource {


    @Test
    public void testUserDefinedSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new EventGenerator())
                .print();

        env.execute();

    }

    public static class EventGenerator implements SourceFunction<Event>, CheckpointedFunction {

        // 是否结束数据生成标志位,多线程下依旧能够正确结束数据模拟，避免线程间isRunning的不可见性
        private volatile boolean isRunning ;
        // 生成的数据记录数控制
        private Long count ;
        // 下一条数据的时间戳
        private volatile Long nextTimestamp;
        // 每条数据生成时间间隔
        private final Integer DURATION_PER_RECORD = 100;
        // list存储模拟的数据
        private List<Event> eventList;
        // liststate存储模拟的数据
        private ListState<Event> eventListState;


        public EventGenerator(){
            this.isRunning = true;
            this.count = 0L;
            this.nextTimestamp = 0L;
            this.eventList = new java.util.ArrayList<>();
        }

// =========================================sourceFunction,模拟数据
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Object lock = ctx.getCheckpointLock();
            // 需要状态恢复时
            synchronized (lock) {
                while (isRunning && count < Long.MAX_VALUE) {
                    Event event = new Event();
                    event.timestamp  = event.timestamp + count;
                    ctx.collect(event);
                    eventList.add(event);
                    count = count + 1000;
                    Thread.sleep(DURATION_PER_RECORD);
                }
            }
        }

        @Override
        public void cancel() {
            this.isRunning = false;
        }
// =========================================checkpointFunction,使得source具有状态管理


        /**
         * snapshotState方法在checkpoint时调用，用于将算子状态快照到StateBackend中
         * @param context the context for drawing a snapshot of the operator
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            eventListState.clear();
            for (Event event : eventList) {
                eventListState.add(event);
            }

        }

        /**
         * initializeState方法在算子初始化或者需要恢复状态时调用，用于从StateBackend中恢复算子状态
         * @param context the context for initializing the operator
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("evenListState", Event.class);
            eventListState = context.getOperatorStateStore().getListState(descriptor);
            //是否是状态恢复？
            if (context.isRestored()) {
                for (Event event : eventListState.get()) {
                    eventListState.add(event);
                }
            }
        }
    }
}



