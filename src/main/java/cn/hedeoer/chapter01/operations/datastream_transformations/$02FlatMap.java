package cn.hedeoer.chapter01.operations.datastream_transformations;

import cn.hedeoer.common.datatypes.TaxiRide;
import cn.hedeoer.common.sources.TaxiRideGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class $02FlatMap {
    /**
     * 将乘车记录中的passengerNames字符串数组（乘客名单）展开，得到行程id, 乘客名单， 每个乘客姓名
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiRide> source = env.addSource(new TaxiRideGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/

        source.flatMap(new FlatMapFunction<TaxiRide, Tuple3<Long, String[], String>>() {
            @Override
            public void flatMap(TaxiRide value, Collector<Tuple3<Long, String[], String>> out) throws Exception {
                long rideId = value.rideId;
                for (int i = 0; i < value.passengerNames.length; i++) {
                    out.collect(Tuple3.of(rideId, value.passengerNames, value.passengerNames[i]));
                }
            }
        })
                        .print();

/*
*
23> (89,[Yvonne, Alice, Charlie, Helen],Yvonne)
23> (89,[Yvonne, Alice, Charlie, Helen],Alice)
23> (89,[Yvonne, Alice, Charlie, Helen],Charlie)
23> (89,[Yvonne, Alice, Charlie, Helen],Helen)
* */
        env.execute();
    }
}
