package cn.hedeoer.common.sink;

import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class TaxiRideSink {
    public static PrintSinkFunction taxiRideSink(){

        return new PrintSinkFunction<>();
    }
}
