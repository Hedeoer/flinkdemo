package cn.hedeoer.chapter01.state;

// 状态的生命周期设定

import cn.hedeoer.chapter01.operations.datastream_transformations.windows.$10GlobalWindows;
import cn.hedeoer.common.datatypes.TaxiRide;
import cn.hedeoer.common.sources.TaxiRideGenerator;
import cn.hedeoer.common.utils.TimeFormat;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.ttl.TtlUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;

/**
 * State Time-To-Live (TTL)
 * 1. 目前只支持 processtime,不支持eventtime，
 * 2. 过期状态的清理
 * ①全量清理
 * ②增量清理
 * ③在RocksDB 合并时清理
 */
public class $10StateTimeToLive {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiRide> source = env.addSource(new TaxiRideGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/

        source
                .keyBy(ride -> ride.driverId)
                .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
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

    public static class MyProcessWindowFunction extends ProcessWindowFunction<TaxiRide, Tuple3<Long,String, Long>, Long, TimeWindow> {


            // <rideId，次数>
            private MapState<Long, Integer> rideIdMap;
            private Integer rideIdNums;

            @Override
            public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, Integer> mapStateDescriptor = new MapStateDescriptor<>("rideIdMap", Long.class, Integer.class);

            rideIdNums = 0;
            rideIdMap = getRuntimeContext().getMapState(mapStateDescriptor);

            // ttl设置
            StateTtlConfig stateTtlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(2))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 何时更新ttl的时间；此处设置如果出现读取或者更新，将更新状态的ttl时间
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 状态过期后是否可读；此处设置为never，过期后不可读
                    .build();
            // ttl用于至状态
            mapStateDescriptor.enableTimeToLive(stateTtlConfig);


        }



        @Override
        public void process(Long driverId, ProcessWindowFunction<TaxiRide, Tuple3<Long,String, Long>, Long, TimeWindow>.Context context, Iterable<TaxiRide> elements, Collector<Tuple3<Long,String, Long>> out) throws Exception {


            for (TaxiRide ride : elements) {
                rideIdMap.put(ride.rideId, 1);
            }
            Iterator<Map.Entry<Long, Integer>> iterator =
                    rideIdMap.iterator();

            Iterable<Integer> values = rideIdMap.values();
            for (Integer value : values) {
                rideIdNums++;
            }
            rideIdMap.clear();


            String triggerTime = TimeFormat.longToString(Instant.now().toEpochMilli());
            out.collect(Tuple3.of(driverId,triggerTime, (long) rideIdNums));

        }
    }


}
