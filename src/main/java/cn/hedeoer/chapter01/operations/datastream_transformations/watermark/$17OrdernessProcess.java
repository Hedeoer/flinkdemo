package cn.hedeoer.chapter01.operations.datastream_transformations.watermark;

import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.datatypes.WaterSensor;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import cn.hedeoer.common.utils.DataUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;



/*
*
* 乱序数据的处理：
* 1. 水印的延迟推进（maxOutOfOrderness）
*  可以在创建水印生成时指定特定的时间作为水印延迟时间
*
* 2. 涉及窗口计算的可以延迟窗口关闭的时间（allowedLateness）
*  窗口计算可以推迟窗口关闭的时间，水印已经到达正常窗口的关闭时间（窗口右边界 + 水印延迟时间），触发一次窗口的计算；若还有落入该窗口的元素且元素事件时间和窗
*  - 窗口正常关闭时间 < allowedLateness，此时符合以上条件，来一条元素，窗口计算一次
*
* 3. 可以使用测流处理迟到的数据，之后在与主流“合并”处理（sideOutputLateData）
*  在以上的条件1和条件3均不能解决元素迟到的问题，则可以使用测输出流接受所有迟到的数据， 根据业务要求合并主流数据和迟到的数据
* */
public class $17OrdernessProcess {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> source = env.socketTextStream("hadoop103", 8989);

        env.setParallelism(1);

        // 创建 测流标签
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("late", TypeInformation.of(WaterSensor.class));

        SingleOutputStreamOperator<List<WaterSensor>> dataStream = source.filter(s -> !s.isEmpty())
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] message = value.split(" ");
                        WaterSensor waterSensor = new WaterSensor();
                        waterSensor.setSensorId(message[0]);
                        waterSensor.setTs(Long.valueOf(message[1]));
                        waterSensor.setWaterLine(Long.valueOf(message[2]));
                        return waterSensor;
                    }
                    // 乱序数据，延迟水位线推进2秒
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }))
                //3秒的滚动全局窗口
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
                // 延迟窗口关闭时间1miao
                .allowedLateness(Time.seconds(1))
                // 侧输出流处理迟到的数据
                .sideOutputLateData(outputTag)
                .process(new ProcessAllWindowFunction<WaterSensor, List<WaterSensor>, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, List<WaterSensor>, TimeWindow>.Context context,
                                        Iterable<WaterSensor> elements,
                                        Collector<List<WaterSensor>> out) throws Exception {
                        // 将Iterable转换成list
                        List<WaterSensor> list = DataUtil.elementToList(elements);
                        out.collect(list);
                    }


                });
        // 打印主流
        dataStream.print();
        // 打印测流
        dataStream.<WaterSensor>getSideOutput(outputTag)
                .printToErr();

        env.execute();

    }
}
