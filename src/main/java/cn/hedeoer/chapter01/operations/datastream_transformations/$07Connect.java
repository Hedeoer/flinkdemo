package cn.hedeoer.chapter01.operations.datastream_transformations;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * connect只可以链接两条流，但是对流的数据类型没有要求
 */
public class $07Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 来自app的支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );

        // 来自第三方支付平台的支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                        return element.f3;
                    }
                })
        );

        appStream.connect(thirdpartStream)
                // 相同的key会进入同一个process
                .keyBy(app -> app.f0,third -> third.f0)
                .process(new MyCoCheckFuncation())
                .print();
        env.execute();
// (order-1,third-party,success,3000)对账成功
//(order-2,app,2000)对账失败,原因第三方支付信息未到达
//(order-3,third-party,success,4000)对账失败，原因app支付信息未到达


    }

    public static class MyCoCheckFuncation extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

        // 状态存储没有匹配到第三方支付信息的 app的支付明细
        private ValueState<Tuple> appStreamState;

        // 状态存储app的支付明细没有匹配到的 第三方支付平台的支付信息
        private ValueState<Tuple> thirdPartStreamState;

        @Override
        public void open(Configuration parameters) throws Exception {
            appStreamState = getRuntimeContext().getState(new ValueStateDescriptor<>("appStreamStateList", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thirdPartStreamState = getRuntimeContext().getState(new ValueStateDescriptor<>("thirdPartStreamStateList", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));


        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            // app的支付信息
            if (thirdPartStreamState.value() != null) {
                thirdPartStreamState.clear();
                out.collect(value + "对账成功");
            }else {
                //第三方支付信息为空，需要将app的支付信息使用状态存储起来，等待5秒后是否出现对应的第三方支付信息
                appStreamState.update(value);
                // 注册5秒后的事件时间定时器
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }

        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {

            // 第三方支付平台的支付信息
            if (appStreamState.value() != null) {
                appStreamState.clear();
                out.collect(value + "对账成功");
            }else{
                // app支付信息为空，需要将第三方支付平台的支付信息使用状态存储起来，等待5秒后是否出现对应的app支付信息
                thirdPartStreamState.update(value);
                ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            if(appStreamState.value() != null){
                out.collect(appStreamState.value() + "对账失败,原因第三方支付信息未到达");
            }

            if (thirdPartStreamState.value() != null){
                out.collect(thirdPartStreamState.value() + "对账失败，原因app支付信息未到达");
            }

            // 已经给了5秒的延迟时间，如果定时器现在触发了，说明数据已经过期了，可以清除状态
            appStreamState.clear();
            thirdPartStreamState.clear();

        }
    }
}
