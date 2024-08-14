package cn.hedeoer.chapter01.faulttolerance;

import cn.hedeoer.chapter01.operations.datastream_transformations.windows.$09ProcessWindowFuncationWithAggregation;
import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class CheckpointConfig {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiFare> source = env.addSource(new TaxiFareGenerator());

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
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        // 将检查点持久化到外部存储
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 如果有更近的保存点时，是否将作业回退到该检查点
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // sets the checkpoint storage where checkpoint snapshots will be written
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink_tolerance");

        // 并行度设置，如何不配置，线上环境，默认任务的并行度为1，即flink-conf.yaml中parallelism.default为1
        env.setParallelism(4);

        processFare(env,source);
        env.execute("Window Processing Example");
    }

    public static void processFare(StreamExecutionEnvironment env,DataStreamSource<TaxiFare> source){

        SingleOutputStreamOperator<Tuple3<Long, Long, Float>> aggregatedStream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.startTime.toEpochMilli()))
                .map(taxi -> Tuple3.of(taxi.driverId, taxi.rideId, taxi.totalFare))
                .returns(Types.TUPLE(Types.LONG, Types.LONG, Types.FLOAT))
                .keyBy(taxi -> taxi.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(5)))  // 使用时间窗口
                .aggregate(new $09ProcessWindowFuncationWithAggregation.MyAggregationFunction(), new $09ProcessWindowFuncationWithAggregation.MyWindowProcessFunction());

        aggregatedStream.map((MapFunction<Tuple3<Long, Long, Float>, Tuple2<Long, Float>>) value -> Tuple2.of(value.f0, value.f2))
                .returns(Types.TUPLE(Types.LONG, Types.FLOAT))
                .print();

    }




    /**
     * 自定义聚合函数，用于处理数据聚合操作
     * 实现了AggregateFunction接口，指定聚合操作的类型为Tuple3(Long, Long, Float)
     */
    public static class MyAggregationFunction implements AggregateFunction<Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>> {

        /**
         * 创建聚合器的初始值
         *
         * @return 返回一个初始化的累积器，用于聚合操作的起始状态
         */
        @Override
        public Tuple3<Long, Long, Float> createAccumulator() {
            return Tuple3.of(0L, 0L, 0.0f);
        }

        /**
         * 将一个值添加到聚合器中
         *
         * @param value       要添加的值，包含聚合操作的数据
         * @param accumulator 当前的聚合器状态
         * @return 返回更新后的聚合器状态
         */
        @Override
        public Tuple3<Long, Long, Float> add(Tuple3<Long, Long, Float> value, Tuple3<Long, Long, Float> accumulator) {
            return Tuple3.of(value.f0, accumulator.f1 + 1, accumulator.f2 + value.f2);
        }

        /**
         * 从聚合器中获取最终的聚合结果
         *
         * @param accumulator 最终状态的聚合器
         * @return 返回聚合结果
         */
        @Override
        public Tuple3<Long, Long, Float> getResult(Tuple3<Long, Long, Float> accumulator) {
            return Tuple3.of(accumulator.f0, accumulator.f1, accumulator.f2);
        }

        /**
         * 合并两个聚合器的状态
         *
         * @param a 第一个聚合器状态
         * @param b 第二个聚合器状态
         * @return 返回合并后的聚合器状态
         */
        @Override
        public Tuple3<Long, Long, Float> merge(Tuple3<Long, Long, Float> a, Tuple3<Long, Long, Float> b) {
            return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }

    /**
     * 自定义窗口处理函数，用于计算窗口内数据的平均值
     * 该函数接收和输出的数据类型均为Tuple3<Long, Long, Float>，分
     * 键值类型为Long，窗口为TimeWindow
     */
    public static class MyWindowProcessFunction
            extends ProcessWindowFunction<Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow> {

        /**
         * 处理函数，用于计算窗口内行程数据的平均费用
         *
         * @param key      窗口的键值，用于分组
         * @param context  上下文对象，可以获取窗口的起止时间等信息
         * @param elements 窗口内的数据元素，每个元素包含三个字段：driverId,rideId,totalFare分别表示 司机ID、行程id和本次车程费用
         * @param out      输出收集器，用于输出计算结果
         */
        @Override
        public void process(Long key, Context context, Iterable<Tuple3<Long, Long, Float>> elements, Collector<Tuple3<Long, Long, Float>> out) {
            // 车程次数
            long count = elements.iterator().next().f1;
            // 总费用,aggregationfunction中已经累计了每个driverid的totalfare
            float totalFare = elements.iterator().next().f2;

            // 计算平均费用
            float averageFare = totalFare / count;
            // 输出键值、计数和平均费用的元组
            out.collect(Tuple3.of(key, count, averageFare));
        }
    }

}
