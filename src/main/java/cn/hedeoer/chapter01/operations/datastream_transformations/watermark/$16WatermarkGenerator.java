package cn.hedeoer.chapter01.operations.datastream_transformations.watermark;

import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/*
 * 水印的生成基于事件时间
 * 需要考虑的问题：
 * 1. 乱序数据如何处理
 *
 * 2. 上游是多并行度的task对来自多个上游task的水印如何取舍
 *  取上游多个水印中最小的水印作为当前task的水印
 *
 * 3. 水印如何生成
 *  ①当前数据流是有序的，还是乱序的
 *  ②水印的提取（会和Long类型的最小值比较，取两者其中最大的一个）
 *  ③水印的发送的周期
 *      周期新的，间隔性的
 *
 * 4. flink内置的水印策略
 *  forMonotonousTimestamps，水印单调递增，发送周期为200ms
 *  forBoundedOutOfOrderness 水印允许乱序，可以设置最大的乱序程度
 *
 *
 * 5.自定义水印的生成策略
 *  实现WatermarkGenerator接口
 *
 * 6. 水印的分发位置
 *  离数据源越近越好，甚至可以在source算子中生成水印
 */
public class $16WatermarkGenerator {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 模拟的source数据频率为每20ms一条，为及时保证水印的更新，所以将自动水印周期设置为5ms
        conf.setString("pipeline.auto-watermark-interval","5");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<TaxiFare> source = env.addSource(new TaxiFareGenerator());
        env.setParallelism(1);


        // 采用乱序水印生成策略，设置乱序程度为0，相当于forMonotonousTimestamps(针对有序流的水印策略)
        source.assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//        source.assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                .withTimestampAssigner((element, recordTimestamp) -> element.startTime.toEpochMilli() ))
                        .process(new ProcessFunction<TaxiFare, Tuple2<String, String>>() {
                            @Override
                            public void processElement(TaxiFare value, ProcessFunction<TaxiFare, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                                String timeEvent = value.startTime.toEpochMilli() + "";
//                                String timeEvent = value.startTime.toString();
//                                String waterMark = TimeFormat.longToString(ctx.timerService().currentWatermark(),true) ;
                                String waterMark = ctx.timerService().currentWatermark() + "";
                                out.collect(Tuple2.of(timeEvent, waterMark));
                            }
                        })
                .print();
//(1577880020000,-9223372036854775808)
//(1577880040000,1577880019999)
//(1577880060000,1577880039999)
//(1577880080000,1577880059999)
//(1577880100000,1577880079999)
//(1577880120000,1577880099999)
//(1577880140000,1577880119999)
        env.execute();


    }
}
