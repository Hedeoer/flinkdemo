package cn.hedeoer.chapter01.operations.datastream_transformations.triggers;


import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 对flink内置的ContinuousProcessingTimeTrigger的模拟效果
 */
public class $11TriggerTest {
    /**
     * 测试自定义触发器,每2秒触发一次
     * 基于处理时间
     * 需求：每2秒计算一次截止当前每次行程的总收入
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiFare> source = env.addSource(new TaxiFareGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/

        source.map(taxi -> (int)taxi.totalFare)
                // 全局窗口
                .windowAll(GlobalWindows.create())
                // 自定义触发器
                .trigger(new CustomTimeTrigger(2000))// 使用自定义时间触发器
                // 窗口函数
                .reduce((a, b) -> a + b)
                .print().setParallelism(1);
        //6329
        //16892
        //26974
        //37822
        //48317
        //58565
        //69472
        //80419

        env.execute();
    }

    public static class CustomTimeTrigger extends Trigger<Integer, GlobalWindow> {
        // 触发器触发的时间间隔
        private final long interval;

        // 单值状态，存储基于处理时间 定时器的触发时间
         ValueStateDescriptor<Long> stateDesc = new ValueStateDescriptor<>("fire-time", LongSerializer.INSTANCE);

        public CustomTimeTrigger(long interval) {
            this.interval = interval;
        }

        //  is called for each element that is added to a window.
        // 窗口内每进入一个元素执行一次onElement（）方法
        // 该方法内从状态中获取定时器的触发时间，若还没有状态，计算触发时间并更新状态；若有状态了，则什么也不做（TriggerResult.CONTINUE：表示触发器不做任何事情）
        // 基于当时处理时间计算下次的定时器触发时间方法：
        // long start = timestamp - (timestamp % interval);
        // ong nextFireTimestamp = start + interval; 使得在interval时间内不重复定义定时器
        @Override
        public TriggerResult onElement(Integer element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            ValueState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);

            timestamp = ctx.getCurrentProcessingTime();

            if (fireTimestamp.value() == null) {
                long start = timestamp - (timestamp % interval);
                long nextFireTimestamp = start + interval;

                ctx.registerProcessingTimeTimer(nextFireTimestamp);

                fireTimestamp.update(nextFireTimestamp);
                return TriggerResult.CONTINUE;
            }
            return TriggerResult.CONTINUE;
        }

        // is called when a registered processing-time timer fires.
        // 当基于处理时间的定时器触发时执行onProcessingTime（）方法，
        // 该方法内从状态中获取定时器的触发时间，若触发时间等于当前时间，先清除状态，之后更新定时器的下次触发时间，并注册相应定时器，触发窗口的计算函数计算窗口元素（在本例中即使用reduce((a, b) -> a + b)）计算全局窗口内的总收入；
        // 若触发时间不等于当前时间，则什么也不做（TriggerResult.CONTINUE：表示触发器不做任何事情）
        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            ValueState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);

            if (fireTimestamp.value().equals(time)) {
                fireTimestamp.clear();
                fireTimestamp.update(time + interval);
                ctx.registerProcessingTimeTimer(time + interval);
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        // is called when a registered event-time timer fires.
        // // 当基于事件时间的定时器触发时执行 onEventTime（）方法，，由于本例基于处理时间，所以返回TriggerResult.CONTINUE，表示什么也不做
        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        // performs any action needed upon removal of the corresponding window.
        // 当窗口生命周期到时执行clear（）方法，清除状态，并删除定时器
        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            // State could be merged into new window.
            ValueState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
            Long timestamp = fireTimestamp.value();
            if (timestamp != null) {
                ctx.deleteProcessingTimeTimer(timestamp);
                fireTimestamp.clear();
            }

        }



//        private static class Min implements ReduceFunction<Long> {
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Long reduce(Long value1, Long value2) throws Exception {
//                return Math.min(value1, value2);
//            }
//        }
    }
}
