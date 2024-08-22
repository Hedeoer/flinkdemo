package cn.hedeoer.chapter01.operations.datastream_transformations;

import cn.hedeoer.common.datatypes.TaxiRide;
import cn.hedeoer.common.sources.TaxiRideGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class $01Map {

    /**
     * 更新每条乘车记录中的乘车人数，passengerCnt（只考虑了乘客的人数），现在需要每次车程的乘车人数（需要考虑司机 ） =  1 + passengerCnt
     * 返回tuple.of(String rideId, Integer passengerCnt,  Integer totalPassages)
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiRide> source = env.addSource(new TaxiRideGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/

        source.map(new MapFunction<TaxiRide, Tuple3<Long, Short, Short>>() {
            @Override
            public Tuple3<Long, Short, Short> map(TaxiRide value) throws Exception {
                Short totalPassages = (short) (value.passengerCnt + 1);
                return Tuple3.of(value.rideId, value.passengerCnt, totalPassages);
            }
        })
        .print();
//        12> (110,3,4)
//        20> (115,3,4)
//        18> (48,3,4)
//        22> (112,3,4)
//        16> (108,3,4)

        env.execute();
    }

}
