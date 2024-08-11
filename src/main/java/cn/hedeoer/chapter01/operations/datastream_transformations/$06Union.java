package cn.hedeoer.chapter01.operations.datastream_transformations;

import cn.hedeoer.common.datatypes.Event;
import cn.hedeoer.common.sources.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * union的数据：数据流中的类型必须一致。
 */
public class $06Union {
    // 定义三条测流标签
    private static OutputTag<Event> BobStream = new OutputTag<Event>("BobStream") {};
    private static OutputTag<Event> CaryStream = new OutputTag<Event>("CaryStream") {};
    private static OutputTag<Event> MaryStream = new OutputTag<Event>("MaryStream") {};


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> process = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp))
                .process(new ProcessFunction<Event, Event>() {
                    // 在process中使用context输出数据到测流
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                        if (value.user.equals("Bob")) {
                            ctx.output(BobStream, value);
                        } else if ("Cary".equals(value.user)) {
                            ctx.output(CaryStream, value);
                        } else if ("Mary".equals(value.user)){
                            ctx.output(MaryStream, value);
                        }else{
                            out.collect(value);
                        }
                    }
                });
// 使用union合并三条类型相同的测流
        process.getSideOutput(BobStream)
                .union(process.getSideOutput(CaryStream), process.getSideOutput(MaryStream))
                .print();

        env.execute();

    }
}
