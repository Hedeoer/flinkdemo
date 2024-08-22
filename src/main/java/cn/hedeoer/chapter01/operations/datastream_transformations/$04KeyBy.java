package cn.hedeoer.chapter01.operations.datastream_transformations;

import cn.hedeoer.common.datatypes.TaxiRide;
import cn.hedeoer.common.sources.TaxiRideGenerator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.expressions.In;

public class $04KeyBy {
    /**
     * 将车程记录按照乘客人数分组
     * keyBy使用的注意事项：
     * 1. key的类型必须是区分不同的，在java中，表现在是否重写hashcode方法，是否重写，取决于对java实列不同的业务定义
     * 2. 如果key的类型是数组， 当前的版本是不可以使用keyBy算子的
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiRide> source = env.addSource(new TaxiRideGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/


        // 按照乘客数量分组
        KeyedStream<TaxiRide, Integer> taxiRideIntegerKeyedStream = source.keyBy(new KeySelector<TaxiRide, Integer>() {
            @Override
            public Integer getKey(TaxiRide value) throws Exception {
                return value.passengerNames.length;
            }
        });
        // 过滤出乘客数量为3的
        taxiRideIntegerKeyedStream.filter(taxiRide -> taxiRide.passengerNames.length == 3).print();
        // 打印的分区号一致都为22，表示是来自同一个分组的数据
//22> 23,START,2020-01-01T12:07:40Z,-73.964935,40.63554,-73.81388,40.827785,3,2013000027,2013000027,Charlie-David-Maria
//22> 58,START,2020-01-01T12:19:20Z,-73.92237,40.689713,-73.832054,40.804653,3,2013000122,2013000122,Victor-Sarah-Helen
//22> 123,START,2020-01-01T12:41:00Z,-73.97387,40.624172,-73.83398,40.80221,3,2013000182,2013000182,Laura-Rachel-Helen
//22> 135,START,2020-01-01T12:45:00Z,-73.92254,40.689495,-73.7673,40.887066,3,2013000166,2013000166,Kevin-Laura-George
//22> 58,END,2020-01-01T13:02:20Z,-73.92237,40.689713,-73.832054,40.804653,3,2013000122,2013000122,James-Quinn-Helen
//22> 214,START,2020-01-01T13:11:20Z,-73.957634,40.64483,-73.84096,40.79333,3,2013000167,2013000167,George-Nancy-Frank
//22> 225,START,2020-01-01T13:15:00Z,-73.946884,40.658516,-73.829735,40.80761,3,2013000167,2013000167,James-David-Peter

        // key的类型为数组类型，即将报错：Type BasicArrayTypeInfo<String> cannot be used as key. Contained UNSUPPORTED key types:
//        source.keyBy(new KeySelector<TaxiRide, String[]>() {
//            @Override
//            public String[] getKey(TaxiRide value) throws Exception {
//                return value.passengerNames;
//            }
//        }).print();
                env.execute();
    }
}
