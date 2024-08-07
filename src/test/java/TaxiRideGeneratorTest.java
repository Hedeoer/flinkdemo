import cn.hedeoer.common.datatypes.TaxiRide;
import cn.hedeoer.common.sources.TaxiRideGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class TaxiRideGeneratorTest {
    private  final SourceFunction<TaxiRide> source;
    private  final SinkFunction<TaxiRide> sink;


    public TaxiRideGeneratorTest(SourceFunction<TaxiRide> source, SinkFunction<TaxiRide> sink) {
        this.source = source;
        this.sink = sink;
    }


    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(source).addSink(sink);
        return env.execute();
    }

    public static void main(String[] args) throws Exception {
        TaxiRideGeneratorTest job = new TaxiRideGeneratorTest(new TaxiRideGenerator(), new PrintSinkFunction<>());
        job.execute();
    }
}
