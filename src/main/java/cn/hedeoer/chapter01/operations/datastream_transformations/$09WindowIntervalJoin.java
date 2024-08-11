package cn.hedeoer.chapter01.operations.datastream_transformations;

import cn.hedeoer.common.datatypes.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * window interval join
 * 语法：
 * keyedStream.intervalJoin(otherKeyedStream)
 * .between(Time.milliseconds(-2), Time.milliseconds(2)) // lower and upper bound
 * .upperBoundExclusive(true) // optional
 * .lowerBoundExclusive(true) // optional
 * .process(new IntervalJoinFunction() {...});
 *
 * 使用
 * ①必须基于keybyStream才可以使用
 * ②key相同并且处于otherKeyedStream约束的的时间区间内，才会触发join计算
 * ③只有join上的结果才会输出
 *
 */
public class $09WindowIntervalJoin {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Tuple3<String, String, Long>, String> orderStream = env.fromElements(
                        Tuple3.of("Mary", "order-1", 5000L),
                        Tuple3.of("Alice", "order-2", 5000L),
                        Tuple3.of("Bob", "order-3", 20000L),
                        Tuple3.of("Alice", "order-4", 20000L),
                        Tuple3.of("Cary", "order-5", 51000L)
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
                )
                .keyBy(data -> data.f0);

        KeyedStream<Event, String> clickStream = env.fromElements(
                        new Event("Bob", "./cart", 2000L),
                        new Event("Alice", "./prod?id=100", 3000L),
                        new Event("Alice", "./prod?id=200", 3500L),
                        new Event("Bob", "./prod?id=2", 2500L),
                        new Event("Alice", "./prod?id=300", 36000L),
                        new Event("Bob", "./home", 30000L),
                        new Event("Bob", "./prod?id=1", 23000L),
                        new Event("Bob", "./prod?id=3", 33000L)
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
                .keyBy(data -> data.user);

        //用户点击之后并在之后10秒内下单的用户明细数据
        orderStream.intervalJoin(clickStream)
                .between(Time.seconds(0), Time.seconds(10))
//                .lowerBoundExclusive()
//                .upperBoundExclusive()
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left,
                                               Event right,
                                               ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>.Context ctx,
                                               Collector<String> out) throws Exception {

                        out.collect(right + "===>" + left);

                    }
                })
                .print();
// Event{user='Bob', url='./home', timestamp=1970-01-01 08:00:30.0}===>(Bob,order-3,20000)
//Event{user='Bob', url='./prod?id=1', timestamp=1970-01-01 08:00:23.0}===>(Bob,order-3,20000)
        env.execute();

    }
}
