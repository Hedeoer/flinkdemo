package cn.hedeoer.chapter01.state;

import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import cn.hedeoer.common.utils.TimeFormat;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// Keyed State 之 ReducingState
public class $03ReduceState {


    public static final ProcessWindowFunction<TaxiFare, Tuple4<Long, String, String, Double>, Long, TimeWindow> MyProcessFunction
            = new ProcessWindowFunction<TaxiFare, Tuple4<Long, String, String, Double>, Long, TimeWindow>() {

        private ReducingState<DriverFareAna> driverFare;

        @Override
        public void open(Configuration parameters) throws Exception {

            driverFare = getRuntimeContext().getReducingState(new ReducingStateDescriptor<DriverFareAna>("driverFare", new MyReduceFunction(), DriverFareAna.class));
        }

        @Override
        public void process(Long aLong,
                            ProcessWindowFunction<TaxiFare, Tuple4<Long, String, String, Double>, Long, TimeWindow>.Context context,
                            Iterable<TaxiFare> elements,
                            Collector<Tuple4<Long, String, String, Double>> out) throws Exception {

            DriverFareAna state = driverFare.get();

            for (TaxiFare driver : elements) {
                DriverFareAna tmp = new DriverFareAna();
                tmp.diverId = driver.driverId;
                tmp.rideIdCount = 1L;
                tmp.curTotalFare = driver.totalFare;
                driverFare.add(tmp);
            }

            String start = TimeFormat.longToString(context.window().getStart());
            String end = TimeFormat.longToString(context.window().getEnd());

            out.collect(Tuple4.of(aLong, start, end, (double) (driverFare.get().curTotalFare / driverFare.get().rideIdCount)));

            driverFare.clear();
        }
    };

    private static class MyReduceFunction implements ReduceFunction<DriverFareAna> {
        @Override
        public DriverFareAna reduce(DriverFareAna value1, DriverFareAna value2) throws Exception {
            DriverFareAna tmp = new DriverFareAna();
            tmp.diverId = value1.diverId;
            tmp.rideIdCount = value1.rideIdCount + value2.rideIdCount;
            tmp.curTotalFare = value1.curTotalFare + value2.curTotalFare;
            return tmp;
        }
    }

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
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//                        .withTimestampAssigner((f, t) -> f.getEventTimeMillis()))
                .keyBy(f -> f.driverId)
//                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(MyProcessFunction)
                .print();

        env.execute();
    }

    //司机的行程收费情况
    private static class DriverFareAna {
        public Long diverId;
        public Long rideIdCount;
        public Float curTotalFare;

        public DriverFareAna() {
        }


    }
}
