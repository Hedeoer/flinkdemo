package cn.hedeoer.chapter01.operations.datastream_transformations;

import cn.hedeoer.common.datatypes.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Window CoGroup 算子相比与 windows join算子， cogroup的输出的结果可以保留没有join上的数据，实现 sql中的 join， left join， right join效果
 * 更加灵活，如果需要更加粒度的控制 join效果，可以使用 connect算子，结合状态，定时器等实现。
 *
 * cogroup机制：
 * 窗口划分：两个输入流会被划分到相同的窗口中，这取决于你定义的窗口类型（如滚动窗口、滑动窗口、会话窗口等）。
 * 按键分组：在每个窗口内，Flink 会根据你指定的键将两个流的数据分组。也就是说，同一个键的元素会被放在一起，并且都位于相同的窗口内。
 * CoGroupFunction 调用：当窗口结束时，Flink 会调用 CoGroupFunction 的 coGroup 方法，传递两个流中相同键的元素集合。此时，传入的元素集合是属于同一个窗口内的
 */
public class $10CoGroup {
    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream = env.fromElements(
                Tuple3.of("Mary", "order-1", 5000L),
                Tuple3.of("Alice", "order-2", 5000L),
                Tuple3.of("Bob", "order-3", 20000L),
                Tuple3.of("Alice", "order-4", 20000L),
                Tuple3.of("Cary", "order-5", 51000L),
                Tuple3.of("Hedeoer", "order-6", 52000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );

        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 36000L),
                new Event("Bob", "./home", 30000L),
                new Event("Bob", "./prod?id=1", 23000L),
                new Event("Bob", "./prod?id=3", 33000L),
        new Event("Jimly", "./prod?id=3", 34000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );

        orderStream.coGroup(clickStream)
                // orderStream 按照 用户 分组
                .where(o1 -> o1.f0)
                // clickStream 也 按照 用户 分组
                .equalTo(c1 -> c1.user)
                // 两个流中相同key的数据放入同一个滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                // 当窗口触发计算， 一次性调用CoGroupFunction
                .apply(new CoGroupFunction<Tuple3<String, String, Long>, Event, String>() {



                    /**
                     * 可以获取到两个流中的所有数据，这里实现left join的效果
                     * @param orders The records from the first input.
                     * @param clicks The records from the second.
                     * @param out A collector to return elements.
                     * @throws Exception
                     */
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Long>> orders, Iterable<Event> clicks, Collector<String> out) throws Exception {
                        // 以左流为基础
                        for (Tuple3<String, String, Long> order : orders) {
                            boolean flag  =false;
//                            能够匹配上右流
                            for (Event click : clicks) {
                                out.collect(order.f0 + " " + order.f1  + " " +  click.url);
                                flag = true;
                            }
//                           不能匹配右流的部分，以空字符表示
                            if (!flag) {
                                out.collect(order.f0 + " " + order.f1  + " " );
                            }
                        }
                    }
                })
                .print();

        env.execute();


    }
}
