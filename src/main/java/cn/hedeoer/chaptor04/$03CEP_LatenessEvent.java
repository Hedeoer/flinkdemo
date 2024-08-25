package cn.hedeoer.chaptor04;

import cn.hedeoer.common.datatypes.TaxiFare;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 匹配后的跳过策略
 */
public class $03CEP_LatenessEvent {

    private static StreamExecutionEnvironment env;
    private static SingleOutputStreamOperator<TaxiFare> source;

    @BeforeAll
    public static void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Set the timestamp extractor and watermark generator
        source = env.readTextFile("input/taxifare.tsv")
                .map(new MapFunction<String, TaxiFare>() {
                    @Override
                    public TaxiFare map(String line) throws Exception {
                        String[] contents = line.split("\t");
                        return new TaxiFare(
                                Long.parseLong(contents[0]),
                                Long.parseLong(contents[1]),
                                Long.parseLong(contents[2]),
                                Instant.parse(contents[3]),
                                contents[4],
                                Float.parseFloat(contents[5]),
                                Float.parseFloat(contents[6]),
                                Float.parseFloat(contents[7]),
                                contents[8]
                        );
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TaxiFare>() {
                    @Override
                    public long extractAscendingTimestamp(TaxiFare element) {
                        return element.startTime.toEpochMilli();
                    }
                });
    }


    /**
     * cep中对迟到数据的处理
     * 方式1. 定义测流处理
     * 方式2：在处理命中策略的方法PatternProcessFunction中实现TimedOutPartialMatchHandler 接口，在处理命中策略的数据同时处理超时数据
     */
    @Test
    public void LatenessEventHandle() {
        Pattern<TaxiFare, TaxiFare> pattern = Pattern.<TaxiFare>begin("start")
                .where(new SimpleCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare value) throws Exception {
                        return "JPY".equals(value.currency);
                    }
                })
                .within(Time.minutes(5));

        CEP.pattern(source, pattern)
                .process(new myPatternProcessFunction())
                .print();


    }

    @AfterAll
    public static void after() throws Exception {
        // Ensure the Flink job is executed
        env.execute();
    }


    private static class myPatternProcessFunction extends PatternProcessFunction<TaxiFare, List<Long>> implements TimedOutPartialMatchHandler<TaxiFare> {
        @Override
        public void processMatch(Map<String, List<TaxiFare>> match, Context ctx, Collector<List<Long>> out) throws Exception {
            List<TaxiFare> start = match.get("start");
            ArrayList<Long> list = new ArrayList<>();
            for (TaxiFare taxiFare : start) {
                list.add(taxiFare.rideId);
            }
            out.collect(list);
        }

        @Override
        public void processTimedOutMatch(Map<String, List<TaxiFare>> match, Context ctx) throws Exception {

        }
    }
}
