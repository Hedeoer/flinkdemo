package cn.hedeoer.chaptor06;

import cn.hedeoer.common.datatypes.Heartbeat;
import cn.hedeoer.common.sources.HeartbeatSource;
import cn.hedeoer.common.utils.TimeFormat;
import org.apache.calcite.avatica.proto.Common;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Timer的使用
 * 1. 定时器触发的时机：当使用事件时间注册的定时器，这会在水印推进到定义的触发时间会触发，执行onTimer方法；使用processing时间注册的定时器，会在处理时间推进到定义的触发时间会触发，执行onTimer方法
 * 2. 定时器的销毁，和合并：定时器也是状态的一部分，过多的定时器会消耗过多的资源，需要注意定时器的销毁和合并。详见：https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/dev/datastream/operators/process_function/#timer-coalescing
 * 3. 定时器使用场景：它通常用于清理缓存、触发窗口计算或执行其他基于时间的任务
 */
public class $01TimerImplements {

    /**
     * 进行心跳检测,如果机器的心跳时间超过10秒，则认为机器异常
     * @param args args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<cn.hedeoer.common.datatypes.Heartbeat> heartbeatDataStreamSource = env.addSource(new HeartbeatSource(1L, 1000L, 1003L, 100.0, 100.0, 100.0, 100.0));

//        heartbeatDataStreamSource.print();

        heartbeatDataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Heartbeat>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((e, l) -> e.reportTime.atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli()))
                .keyBy(heartbeat -> heartbeat.hostId)
                .process(new HeartBeatCheckProcessFunction())
                .print();


        // 5. 执行作业
        env.execute("DataGen Heartbeat to DataStream");
    }

    /**
     * valueState 保存较前的过期时间。
     * 0. 程序初始化时，初始化单值状态
     * 1. 元素第一次到来，登记基于事件时间的定时器（10秒后）,登记超时时间
     * 2. 元素之后每次到来，删除之前的定时器，再次注册基于本次事件时间的定时器
     * 3. 当定时器一旦触发，表示机器心跳超时，需要做一些告警信息
     *
     *
     */
    private static class HeartBeatCheckProcessFunction extends KeyedProcessFunction<Long, Heartbeat, String> {

        private ValueState<Long> timeoutState;

        @Override
        public void open(Configuration parameters) {
            // 使用更清晰的变量名
            timeoutState = getRuntimeContext().getState(new ValueStateDescriptor<>("timeoutState", Long.class));
        }

        @Override
        public void processElement(Heartbeat value, Context ctx, Collector<String> out) throws Exception {
            // 1. 获取状态中存储的上一个定时器的时间戳
            Long previousTimeout = timeoutState.value();

            // 2. 如果状态不为 null，说明之前已经注册过定时器，需要先删除它
            if (previousTimeout != null) {
                ctx.timerService().deleteEventTimeTimer(previousTimeout);
            }

            // 3. 收到新的心跳，说明机器当前是正常的
            out.collect("机器正常：" + value.hostId + "，在事件时间 " + TimeFormat.longToString(ctx.timestamp()));

            // 4. 基于当前元素的事件时间 (ctx.timestamp())，注册一个新的 10 秒后的定时器
            long newTimeout = ctx.timestamp() + 10000L;
            ctx.timerService().registerEventTimeTimer(newTimeout);

            // 5. 【关键】更新状态，保存这个新的定时器时间戳
            timeoutState.update(newTimeout);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 严谨性检查：仅当触发的定时器时间与状态中保存的时间一致时才告警
            // 这可以防止处理已过期的定时器
            if (timeoutState.value() != null && timeoutState.value().equals(timestamp)) {
                out.collect("机器异常：" + ctx.getCurrentKey() + "，在事件时间 " + TimeFormat.longToString(timeoutState.value()) + " 时没有收到心跳！");

                // 【关键】告警后清空状态，让这个 key 恢复到初始状态
                timeoutState.clear();
            }
        }
    }
}
