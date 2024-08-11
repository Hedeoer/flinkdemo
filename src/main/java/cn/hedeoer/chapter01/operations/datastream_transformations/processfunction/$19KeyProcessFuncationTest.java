package cn.hedeoer.chapter01.operations.datastream_transformations.processfunction;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class $19KeyProcessFuncationTest {
    public static void main(String[] args) throws Exception {

        // 创建配置对象
        Configuration config = new Configuration();
        // 设置Flink Web UI的端口号为 9423
        config.setInteger(RestOptions.PORT, 8081);;
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment(config);
        DataStreamSource<String> source = evn.socketTextStream("hadoop103", 7777);

        evn.setParallelism(2);


        source.filter(data -> !data.isEmpty())
                .map(data -> Tuple2.of(data.split(" ")[0], Long.parseLong(data.split(" ")[1])))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1))
                .keyBy(data -> data.f0)
                .process(new MyProcessFuncation())
                .print();


        evn.execute();
    }

    /**
     *
     */
    private static class MyProcessFuncation extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {

        // 存储每个key的值，出现次数，和上次出现的时间
        private ValueState<CountWithTimeStamp> counter;

        // 每个key处理前的初始化
        @Override
        public void open(Configuration parameters) throws Exception {super.open(parameters);
            counter = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimeStamp>("count", CountWithTimeStamp.class));
        }

        // 处理每个key数据
        @Override
        public void processElement(Tuple2<String, Long> value,
                                   KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.Context ctx,
                                   Collector<Tuple2<String, Long>> out) throws Exception {
            CountWithTimeStamp countWithTimeStamp = counter.value();
            if (countWithTimeStamp == null) {
                countWithTimeStamp  = new CountWithTimeStamp();
                countWithTimeStamp.key = value.f0;
                countWithTimeStamp.count = 0L;
                countWithTimeStamp.timestamp = 0L;
            }

            ++ countWithTimeStamp.count;
            countWithTimeStamp.timestamp = ctx.timestamp();

            counter.update(countWithTimeStamp);

            ctx.timerService().registerProcessingTimeTimer(countWithTimeStamp.timestamp + 5000L);
            System.out.println("定义了" +  (countWithTimeStamp.timestamp + 5000L) + "毫秒后的的定时器");

        }


        // 定时器触发时执行

        /**
         *
         * @param timestamp The timestamp of the firing timer.
         * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
         *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
         *     registering timers and querying the time. The context is only valid during the invocation
         *     of this method, do not store it.
         * @param out The collector for returning result values.
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {

            System.out.println("当前时间" + timestamp + "毫秒");
            CountWithTimeStamp countOnTime = counter.value();
            if ((countOnTime.timestamp + 5000L) == timestamp) {

                out.collect(Tuple2.of(countOnTime.key, countOnTime.count));
            }
        }
    }


    private static class CountWithTimeStamp{
        public String key;
        public Long count;
        public Long timestamp;

        public CountWithTimeStamp() {
        }
    }
}
