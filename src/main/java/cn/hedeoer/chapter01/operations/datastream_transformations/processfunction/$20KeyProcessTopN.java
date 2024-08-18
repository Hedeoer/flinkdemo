package cn.hedeoer.chapter01.operations.datastream_transformations.processfunction;

import cn.hedeoer.common.datatypes.Event;
import cn.hedeoer.common.sources.ClickSource;
import cn.hedeoer.common.utils.SinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * KeyedProcessFunction  的使用
 * 每秒统计一次过去10秒内点击热门的top3链接，显示具体链接，访问次数，排序编号
 * 思路：
 * 1. 事件时间的滑动窗口，计算每个链接在窗口内的点击次数。返回链接， 点击次数，窗口起止时间戳
 * 2. 计算每个窗口内的点击量的top3
 */
public class $20KeyProcessTopN {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.addSource(new ClickSource());

        // 测流明细数据的存储位置
        String path = "output/eventStream";
        FileSink<String> sink = SinkUtil.getFileSink(path);

//        / 创建一个侧流输出event数据明细到文件，目的对比之后和窗口统计的结果
        // 定义输出标签
        OutputTag<String> eventStream = new OutputTag<>("eventStream", Types.STRING);
        source.getSideOutput(eventStream)
                        .sinkTo(sink);

        source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp))
                .keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 采用窗口预聚合的计算方法
                .aggregate(new MyAggregation(), new MyAggregationProcess())
                // 以每个窗口的有边界为key分组
                .keyBy(data -> data.f3)
                .process(new TopN())
                .print();
//接受了数据：Event{user='Alice', url='./cart', timestamp=2024-08-11 16:01:05.996}
//接受了数据：Event{user='Bob', url='./prod?id=1', timestamp=2024-08-11 16:01:07.009}
//接受了数据：Event{user='Bob', url='./home', timestamp=2024-08-11 16:01:08.017}
//接受了数据：Event{user='Alice', url='./home', timestamp=2024-08-11 16:01:09.027}
//接受了数据：Event{user='Bob', url='./prod?id=2', timestamp=2024-08-11 16:01:10.039}
//接受了数据：Event{user='Alice', url='./prod?id=1', timestamp=2024-08-11 16:01:11.048}
//16> ./home:2 ./cart:1 ./prod?id=1:1

        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Long, Tuple4<String, Long, Long, Long>, String> {

        // 存储相同的时间窗口内的每个链接的点击次数
        private ListState<Tuple4<String, Long, Long, Long>> urlCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            urlCountListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple4<String, Long, Long, Long>>("TOPN", Types.TUPLE(Types.STRING, Types.LONG, Types.LONG, Types.LONG)));
        }


        // Tuple4<String, Long, Long, Long>的每个元素处理规则
        @Override
        public void processElement(Tuple4<String, Long, Long, Long> value,
                                   KeyedProcessFunction<Long, Tuple4<String, Long, Long, Long>, String>.Context ctx,
                                   Collector<String> out) throws Exception {
            if (value != null) {
                urlCountListState.add(value);
                ctx.timerService().registerEventTimeTimer(value.f3 + 1000);
            }

        }

        /**
         * Tuple4<String, Long, Long, Long>:
         * f0:url
         * f1:点击次数
         * f3:所处计算窗口的开始时间
         * f4:所处计算窗口的结束时间
         * @param timestamp The timestamp of the firing timer.
         * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
         *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
         *     registering timers and querying the time. The context is only valid during the invocation
         *     of this method, do not store it.
         * @param out The collector for returning result values.
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<Long, Tuple4<String, Long, Long, Long>, String>.OnTimerContext ctx,
                            Collector<String> out) throws Exception {

            Iterable<Tuple4<String, Long, Long, Long>> similarWindowUrlCounts = urlCountListState.get();


            String result = StreamSupport.stream(similarWindowUrlCounts.spliterator(), false)
                    .sorted((o1, o2) -> o2.f1.compareTo(o1.f1))
                    .limit(3)
                    .map(o -> o.f0 + ":" + o.f1)
                    .collect(Collectors.joining(" "));

            urlCountListState.clear();

            out.collect(result);

        }
    }

    public static class MyAggregation implements AggregateFunction<Event, UrlCount, UrlCount> {


        @Override
        public UrlCount createAccumulator() {
            return new UrlCount("", 0L);
        }

        @Override
        public UrlCount add(Event value, UrlCount accumulator) {
            accumulator.counts += 1;
            accumulator.url = value.url;
            return accumulator;
        }

        @Override
        public UrlCount getResult(UrlCount accumulator) {
            return accumulator;
        }

        @Override
        public UrlCount merge(UrlCount a, UrlCount b) {
            b.counts = a.counts + b.counts;
            return b;
        }
    }

    public static class MyAggregationProcess extends ProcessWindowFunction<UrlCount, Tuple4<String, Long, Long, Long>, String, TimeWindow> {

        /**
         * @param s        The key for which this window is evaluated. 点击的url
         * @param context  The context in which the window is being evaluated. 处理函数的上下文
         * @param elements The elements in the window being evaluated. 落入process的聚合结果（UrlCount）
         * @param out      A collector for emitting elements.
         * @throws Exception
         */
        @Override
        public void process(String s,
                            ProcessWindowFunction<UrlCount, Tuple4<String, Long, Long, Long>, String, TimeWindow>.Context context,
                            Iterable<UrlCount> elements,
                            Collector<Tuple4<String, Long, Long, Long>> out) throws Exception {

            long start = context.window().getStart();
            long end = context.window().getEnd();

            Iterator<UrlCount> iterator = elements.iterator();
            // 遍历的UrlCount的结果
            while (iterator.hasNext()) {
                UrlCount ur = iterator.next();
                out.collect(Tuple4.of(ur.url, ur.counts, start, end));
            }
        }
    }

    public static class UrlCount {
        private String url;
        private Long counts;

        public UrlCount() {
        }

        public UrlCount(String url, Long counts) {
            this.url = url;
            this.counts = counts;
        }
    }
}
