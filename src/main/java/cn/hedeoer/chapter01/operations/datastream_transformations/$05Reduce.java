package cn.hedeoer.chapter01.operations.datastream_transformations;

import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.datatypes.TaxiRide;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import cn.hedeoer.common.sources.TaxiRideGenerator;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class $05Reduce {

    /**
     * 计算出租车行程数据流中每个司机收取的小费总和，返回 <driverId, totalTip> 元素的流
     * 计算流程：
     * 1. 按照driverId分组
     * 2. 分组内求和小费
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiFare> source = env.addSource(new TaxiFareGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/

        // 使用reduce实现，输入类型 和输出类型一致
        source.keyBy(taxiFare -> taxiFare.driverId)
                .reduce(new ReduceFunction<TaxiFare>() {
                    @Override
                    public TaxiFare reduce(TaxiFare value1, TaxiFare value2) throws Exception {
                        TaxiFare tmp = new TaxiFare();
                        tmp.driverId = value1.driverId;

                        tmp.tip = value1.tip + value2.tip;
                        return tmp;
                    }
                })
                .map(taxiFare -> Tuple2.of(taxiFare.driverId, taxiFare.tip))
                .returns(Types.TUPLE(Types.LONG, Types.FLOAT))
                .print();


//        22> (2013000062,62.0)
//        22> (2013000062,94.0)
//        22> (2013000062,128.0)
//        22> (2013000062,145.0)


        // 使用process实现，输入类型和输出类型可以不一致
        // 实现sql中类似sum(tip) over(partition by driverId order by 记录时间)的效果 todo



        env.execute();


    }
}
