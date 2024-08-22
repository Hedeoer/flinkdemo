package cn.hedeoer.chapter01.operations.datastream_transformations.watermark;

import cn.hedeoer.common.datatypes.WaterSensor;
import cn.hedeoer.common.utils.DataUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

/*
 * 水印的赋闲策略
 * 面对的场景：当task的上下游间，存在上游多个并行度，不断的提取和生成水印，并向下游task广播水印。下游task会取收到的水印中最小的作为当前task的水印。
 * 如果上游的某个subtask出现故障导致水印不会更新，那么可能会导致下游的水印一直保持不变，导致下游task计算。
 *
 * 此时需要设置水印的赋闲策略：
 * 当某个并行度任务（subtask）的水印长时间不更新，且超过了设置的赋闲时间，将取消该subtask向下游发送水印的权利，直到该subtask的水印更新才能再次向下游发送水印。
 * */
public class $18Idlness {
    public static void main(String[] args) throws Exception {

        // 创建配置对象
        Configuration config = new Configuration();

        // 设置Flink Web UI的端口号为 9423
        config.setInteger(RestOptions.PORT, 8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        DataStreamSource<String> source = env.socketTextStream("hadoop103", 8989);
        env.setParallelism(3);


        SingleOutputStreamOperator<List<WaterSensor>> dataStream =
                source

                        .global() // 开启，则filter只向map的第一个任务发送数据，map的其他subtask的水印不会更新，导致下游的 window 无法计算

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

                        }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((water,time) -> Long.valueOf(water.getTs()))
                                // 算子赋闲策略，2秒水印未更新停止向下游发送水印
                                .withIdleness(Duration.ofSeconds(2))
                        )
                        .keyBy(WaterSensor::getSensorId)
                        //3秒的滚动全局窗口
                        .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                                .process(new ProcessWindowFunction<WaterSensor, List<WaterSensor>, String, TimeWindow>() {
                                    @Override
                                    public void process(String s, ProcessWindowFunction<WaterSensor, List<WaterSensor>, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<List<WaterSensor>> out) throws Exception {
                                        // 将Iterable转换成list
                                        List<WaterSensor> list = DataUtil.elementToList(elements);
                                        out.collect(list);
                                    }
                                });
        // 打印主流
        dataStream.print();

        env.execute();

    }
}
