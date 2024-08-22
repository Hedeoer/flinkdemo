package cn.hedeoer.chapter01.operations.datastream_transformations.windows;


import cn.hedeoer.common.datatypes.TaxiRide;
import cn.hedeoer.common.sources.TaxiRideGenerator;
import cn.hedeoer.common.utils.TimeFormat;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.HashSet;


/**
 * ClassName: $10GlobalWindows
 * Description:Global Windows的使用
 * Author: hedeoer
 * 除了global window外，flink中提供的内置window类型（TumblingWindow、SlidingWindow、SessionWindow）都基于时间（事件时间，处理时间）语义，
 * globl window没有边界，所以窗口的触发需要用户手动触发。即需要自定义触发器或者使用flink内置提供的触发器。trigger
 * flink内置提供的触发器类型，可以参考Trigger抽象类的子类
 */
public class $10GlobalWindows {
    /**
     * 每5秒输出一次共有多少司机接到订单
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiRide> source = env.addSource(new TaxiRideGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/

        source
                .windowAll(GlobalWindows.create())
//                .trigger(CountTrigger.of(5)) // 基于元素个数
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))//基于确定的处理时间间隔
//                .trigger(new MyTrigger()) //自定义的触发器
                .process(new MyProcessWindowFunction())
                .print()
                .setParallelism(1);

        env.execute();
        //由于模拟的司机个数上限为200，一段时间后，司机总数不会发生变化
        //(2024-08-09 11:23:25,67)
        //(2024-08-09 11:23:30,200)
        //(2024-08-09 11:23:35,200)
        //(2024-08-09 11:23:40,200)
        //(2024-08-09 11:23:45,200)
        //(2024-08-09 11:23:50,200)

    }


    public static class MyProcessWindowFunction extends ProcessAllWindowFunction<TaxiRide, Tuple2<String,Long>,GlobalWindow>{

        @Override
        public void process(ProcessAllWindowFunction<TaxiRide, Tuple2<String, Long>, GlobalWindow>.Context context,
                            Iterable<TaxiRide> elements,
                            Collector<Tuple2< String, Long>> out) throws Exception {
            HashSet<Long> driverIds = new HashSet<>();
            for (TaxiRide ride : elements) {
                driverIds.add(ride.driverId);
            }

            String triggerTime = TimeFormat.longToString(Instant.now().toEpochMilli());
            out.collect(Tuple2.of(triggerTime, (long) driverIds.size()));

        }
    }




}
