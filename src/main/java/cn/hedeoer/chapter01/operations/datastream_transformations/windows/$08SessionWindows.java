package cn.hedeoer.chapter01.operations.datastream_transformations.windows;

import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import cn.hedeoer.common.utils.SinkUtil;
import cn.hedeoer.common.utils.TimeFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 查询车程收费记录中每个”会话“的活跃司机人数
 */
public class $08SessionWindows {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiFare> source = env.addSource(new TaxiFareGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/

        // 测流明细数据的存储位置
        String path = "output/taxiFareStream";
        FileSink<String> sink = SinkUtil.getFileSink(path);


        // 创建一个侧流输出行程数据明细到文件，目的对比之后和窗口统计的结果
        // 定义输出标签
        OutputTag<String> taxiFareStream = new OutputTag<>("taxiFareStream", Types.STRING);

        source
                .map(taxiFare -> {
                    long driverId = taxiFare.driverId;
                    String eventTime = TimeFormat.longToString(taxiFare.startTime.toEpochMilli());
                    return Tuple2.of(driverId, eventTime);
                })
                .returns(Types.TUPLE(Types.LONG, Types.STRING))
                .process(new ProcessFunction<Tuple2<Long, String>, Tuple2<Long, String>>() {
                    @Override
                    public void processElement(Tuple2<Long, String> value, ProcessFunction<Tuple2<Long, String>, Tuple2<Long, String>>.Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
                        ctx.output(taxiFareStream, value.toString());
                    }
                })
                .getSideOutput(taxiFareStream)
                .sinkTo(sink)
                .setParallelism(1);  // 增加并行度以提高性能


        source.assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((element, recordTimestamp) -> element.startTime.toEpochMilli()))
                // 转化tuple2形式
                .map(taxiFare -> Tuple2.of(taxiFare.driverId, 1l))
                .returns(Types.TUPLE(Types.LONG, Types.LONG))
                // 按照driverId进行分组，减轻window压力
                .keyBy(tupe2 -> tupe2.f0)
                // 基于事件事件的会话窗口，会话定义为两次记录的事件时间相差1小时则属于不同的会话
                // 统计会话内的活跃司机数，不对driverId去重
                // 使用 ProcessWindowFunction with Incremental Aggregation，对每条记录都聚合一次，不用等待窗口关闭时计算堆积在窗口内的元素
                                .window(EventTimeSessionWindows.withGap(Time.hours(1)))
                                        .reduce(new ReduceFunction<Tuple2<Long, Long>>() {
                                            @Override
                                            public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
                                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                                            }
                                        }, new ProcessWindowFunction<Tuple2<Long, Long>, Tuple3<String, String, Long>, Long, TimeWindow>() {
                                            @Override
                                            public void process(Long aLong,
                                                                ProcessWindowFunction<Tuple2<Long, Long>, Tuple3<String, String, Long>, Long, TimeWindow>.Context context,
                                                                Iterable<Tuple2<Long, Long>> elements,
                                                                Collector<Tuple3<String, String, Long>> out) throws Exception {

                                                String start = TimeFormat.longToString(context.window().getStart());
                                                String end = TimeFormat.longToString(context.window().getEnd());
                                                Long count = 0L;
                                                for (Tuple2<Long, Long> ele : elements) {
                                                    count += ele.f1;
                                                }

                                                out.collect(Tuple3.of(start, end, count));
                                            }
                                        }).print("window")
                        .setParallelism(1);

        env.execute();
    }

}
