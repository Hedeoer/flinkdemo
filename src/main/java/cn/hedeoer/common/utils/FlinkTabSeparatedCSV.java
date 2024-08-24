package cn.hedeoer.common.utils;

import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/**
 * 生产taxifare的测试TSV数据，主要用于测试cep
 * 文件标题行为
 * rideId  taxiId  driverId    startTime   paymentType tip tolls   totalFare   currency
 */


public class FlinkTabSeparatedCSV {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<Event> source = env.addSource(new ClickSource());
        DataStreamSource<TaxiFare> source = env.addSource(new TaxiFareGenerator());
        // 设置并行度为1，保证一定时间内产生一个文件即可，用于测试
        env.setParallelism(1);

        //long rideId
        //long taxiId
        //long driverId
        //Instant startTime
        //String paymentType
        //float tip
        //float tolls
        //float totalFare
        //String currency
        // Define the output format (Tab-separated values)

        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("output/taxifare/"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(60 * 1000)
                                .withInactivityInterval(60 * 1000)
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();


        // Apply the transformation to format data as TSV
        SingleOutputStreamOperator<String> result = source.map(fare -> {
            StringBuilder bu = new StringBuilder();
            bu.append(fare.rideId)
                    .append("\t")
                    .append(fare.taxiId)
                    .append("\t")
                    .append(fare.driverId)
                    .append("\t")
                    .append(fare.startTime)
                    .append("\t")
                    .append(fare.paymentType)
                    .append("\t")
                    .append(fare.tip)
                    .append("\t")
                    .append(fare.tolls)
                    .append("\t")
                    .append(fare.totalFare)
                    .append("\t")
                    .append(fare.currency);
            return bu.toString();
        });


        // Add sink to the stream
        result.addSink(sink);

        // Execute the job
        env.execute("Flink Tab-Separated CSV Example");
    }
}
