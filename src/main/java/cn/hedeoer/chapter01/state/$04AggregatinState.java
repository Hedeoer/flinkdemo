package cn.hedeoer.chapter01.state;


import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import cn.hedeoer.common.utils.TimeFormat;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;

// keyState 之 AggregatingState使用，需要的前置知识点mapState的使用
public class $04AggregatinState {
    /**
     * 计算车程记录中的每个司机每个隔10秒的平均收费（10秒内的总收入[totalFare] / 10秒内的行程数[rideId]）
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiFare> source = env.addSource(new TaxiFareGenerator());

        env.setParallelism(1);

        source

                .keyBy(f -> f.driverId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new MyProcessFunction())
                .print();

        env.execute();
    }

    public static class MyProcessFunction extends ProcessWindowFunction<TaxiFare, Tuple4<Long, String, String, Double>, Long, TimeWindow> {

        // AggregatingState，元素为 DriverFareAna《rideIdCount， totalFare》
        private AggregatingState<TaxiFare, DriverFareAna> driverAnalysis;
        // MapState，元素为 rideId，totalFare,对同一个司机在相同时间窗口内的收费情况，按照时间顺序取最后一条，相当于按照 driverId，rideId分组，取处理时间最后的一条
        private MapState<Long, Float> rideIdDistinct;

        @Override
        public void open(Configuration parameters) throws Exception {
            driverAnalysis = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<TaxiFare, DriverFareAna, DriverFareAna>("totalFare", new DriverAggregation(), TypeInformation.of(DriverFareAna.class)));
            rideIdDistinct = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Float>("rideIdDistinct", Long.class, Float.class));

        }

        @Override
        public void process(Long driverId, ProcessWindowFunction<TaxiFare, Tuple4<Long, String, String, Double>, Long, TimeWindow>.Context context, Iterable<TaxiFare> elements, Collector<Tuple4<Long, String, String, Double>> out) throws Exception {
            TaxiFare taxiFare = new TaxiFare();
            DriverFareAna tmp = driverAnalysis.get();

            if (tmp == null) {
                //遍历收费情况，添加到MapState去重
                for (TaxiFare element : elements) {
                    rideIdDistinct.put(element.rideId, element.totalFare);

                }
            } else {
                // ProcessWindowFunction处理的为窗口时间内累计的元素，driverAnalysis不为空，表示到下一次窗口触发计算，需要清空之前的MapState，和 driverAnalysis
                driverAnalysis.clear();
                rideIdDistinct.clear();
                for (TaxiFare element : elements) {
                    rideIdDistinct.put(element.rideId, element.totalFare);
                }

            }

            Iterable<Map.Entry<Long, Float>> entries = rideIdDistinct.entries();
            // 遍历mapState，添加到AggregatingState中
            for (Map.Entry<Long, Float> entry : entries) {
                taxiFare.rideId = entry.getKey();
                taxiFare.totalFare = entry.getValue();
                driverAnalysis.add(taxiFare);
            }

            String start = TimeFormat.longToString(context.window().getStart());
            String end = TimeFormat.longToString(context.window().getEnd());

            DriverFareAna cur = driverAnalysis.get();
            Float averageFare = cur.curTotalFare / cur.rideIdCount;

            out.collect(Tuple4.of(driverId, start, end, Double.valueOf(averageFare)));


        }
    }

    /**
     * TaxiFare:输入值
     * Tuple4<Long, String, String, Double>：输出值
     * DriverFareAna：中间累加器，类型<driverid， ridecounts， totalFare>
     */
    public static class DriverAggregation implements AggregateFunction<TaxiFare, DriverFareAna, DriverFareAna> {
        @Override
        public DriverFareAna createAccumulator() {
            return new DriverFareAna(0L, 0.0f);
        }

        @Override
        public DriverFareAna add(TaxiFare value, DriverFareAna accumulator) {
            accumulator.rideIdCount += 1;
            accumulator.curTotalFare += value.totalFare;
            return accumulator;
        }

        /**
         * 输出rideIdCount和对应的curTotalFare
         *
         * @param accumulator The accumulator of the aggregation
         * @return
         */
        @Override
        public DriverFareAna getResult(DriverFareAna accumulator) {
            return accumulator;
        }

        @Override
        public DriverFareAna merge(DriverFareAna a, DriverFareAna b) {
            b.rideIdCount = a.rideIdCount + b.rideIdCount;
            b.curTotalFare = a.curTotalFare + b.curTotalFare;
            return b;
        }
    }


    //司机的行程收费情况
    private static class DriverFareAna {
        public Long rideIdCount;
        public Float curTotalFare;

        public DriverFareAna() {
        }

        public DriverFareAna(Long rideIdCount, Float curTotalFare) {
            this.curTotalFare = curTotalFare;
            this.rideIdCount = rideIdCount;
        }


    }
}
