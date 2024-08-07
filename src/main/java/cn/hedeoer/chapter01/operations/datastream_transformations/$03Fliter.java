package cn.hedeoer.chapter01.operations.datastream_transformations;

import cn.hedeoer.common.datatypes.TaxiRide;
import cn.hedeoer.common.sources.TaxiRideGenerator;
import cn.hedeoer.common.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class $03Fliter {

    /**
     * 过滤出
     * 1. 车程在纽约市以内开始并且在纽约内结束的车程
     * 2. 车程内乘客数量为4
     * 满足以上2个条件的行程记录
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiRide> source = env.addSource(new TaxiRideGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/

        FilterByNYC filterByNYC = new FilterByNYC();
        source.filter(new FilterFunction<TaxiRide>() {
            @Override
            public boolean filter(TaxiRide value) throws Exception {


                return filterByNYC.filter(value) && value.passengerCnt == 4;
            }
        }).print();
//        9> 2,START,2020-01-01T12:00:40Z,-73.85604,40.77413,-73.80203,40.84287,4,2013000108,2013000108,Kevin-Oliver-Edith-David
//        7> 5,START,2020-01-01T12:01:40Z,-73.85884,40.77057,-73.75468,40.903137,4,2013000087,2013000087,Kevin-Oliver-Charlie-Quinn
//        8> 3,START,2020-01-01T12:01:00Z,-73.86453,40.763325,-73.84797,40.7844,4,2013000134,2013000134,Victor-Sarah-Ursula-Frank
//        10> 4,START,2020-01-01T12:01:20Z,-73.86093,40.767902,-73.781784,40.868633,4,2013000062,2013000062,Bob-Edith-Ursula-Wendy
        env.execute();


    }

    static class FilterByNYC implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide value) throws Exception {
            return GeoUtils.isInNYC(value.startLon, value.startLat) && GeoUtils.isInNYC(value.endLon, value.endLat);
        }
    }

}
