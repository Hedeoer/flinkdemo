package cn.hedeoer.chapter01.operations.datastream_transformations.windows;

import cn.hedeoer.common.datatypes.TaxiRide;
import cn.hedeoer.common.sources.TaxiRideGenerator;
import cn.hedeoer.common.utils.SinkUtil;
import cn.hedeoer.common.utils.TimeFormat;
import com.sun.java.swing.plaf.windows.resources.windows;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static cn.hedeoer.common.utils.SinkUtil.getFileSink;

public class $06TumblingWindows {
    /**
     * 每隔5秒统计一次过去5秒共有多少车程
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiRide> source = env.addSource(new TaxiRideGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/

        // 测流明细数据的存储位置
        String path = "output/taxiRideStream";
        FileSink<String> sink = SinkUtil.getFileSink(path);


        // 创建一个侧流输出行程数据明细到文件，目的对比之后和窗口统计的结果
        // 定义输出标签
        OutputTag<String> taxiRideStream = new OutputTag<>("taxiRideStream", Types.STRING);

        source
                .map(taxiRide -> {
                    long rideId = taxiRide.rideId;
                    String eventTime = TimeFormat.longToString(taxiRide.eventTime.toEpochMilli());
                    return Tuple2.of(rideId, eventTime);
                })
                .returns(Types.TUPLE(Types.LONG, Types.STRING))
                .process(new ProcessFunction<Tuple2<Long, String>, Tuple2<Long, String>>() {
                    @Override
                    public void processElement(Tuple2<Long, String> value, ProcessFunction<Tuple2<Long, String>, Tuple2<Long, String>>.Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
                        ctx.output(taxiRideStream, value.toString());
                    }
                })
                .getSideOutput(taxiRideStream)
                .sinkTo(sink)
                .setParallelism(1);  // 增加并行度以提高性能



        source
                // 提取事件时间作为水印并允许5秒的迟到
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                        .withTimestampAssigner((ride, timestamp) -> ride.eventTime.toEpochMilli()))
                // 滚动全局窗口
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 窗口函数
                .apply(new AllWindowFunction<TaxiRide, Tuple3<String, String, Integer>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TaxiRide> values, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        // hashset存储去重的DriverId
                        HashSet<Long> distinctRideId = new HashSet<>();
                        for (TaxiRide ta : values) {
                            distinctRideId.add(ta.rideId);
                        }
                        // 获取窗口的开始和结束时间
                        String start = TimeFormat.longToString(window.getStart());
                        String end = TimeFormat.longToString(window.getEnd());

                        Tuple3<String, String, Integer> result = new Tuple3<>(start, end, distinctRideId.size());

                        out.collect(result);
                    }
                })
                        .print("windows");
        env.execute();
    }

    // 对比测流输出和 窗口统计输出

// 测流情况
//rideId, eventTime
//(61,2020-01-01 21:00:20) <---
//(181,2020-01-01 21:00:20)<---
//(185,2020-01-01 21:01:40)
//(89,2020-01-01 21:00:40)
//(182,2020-01-01 21:00:40)
//(183,2020-01-01 21:01:00)
//(184,2020-01-01 21:01:20)
//(87,2020-01-01 21:02:00)
//(105,2020-01-01 21:03:00)
//(76,2020-01-01 21:00:20) <---

// 窗口统计情况
// 窗口开始时间，窗口结束时间，窗口内去重后的rideId数量
// windows:9> (2020-01-01 21:00:20,2020-01-01 21:00:25,3)


}
