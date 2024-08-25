package cn.hedeoer.chaptor04;

import cn.hedeoer.common.datatypes.TaxiFare;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
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
public class $03CEP_MatchSkipStrategy {

    private static StreamExecutionEnvironment env;
    private static SingleOutputStreamOperator<TaxiFare> source;

    @BeforeAll
    public static void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Set the timestamp extractor and watermark generator
        source = env.readTextFile("output/taxifare/2024-08-24--19/.part-0-0.inprogress.bcdffd45-8338-4218-ab86-4cd998c2dfc4")
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
     * 匹配后的跳过策略;默认是no_skip，即全部展示
     * 常用的策略还有：skipToNext,skipPastLastEvent
     */
    @Test
    public void skipStrategy() {

//        AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<TaxiFare, TaxiFare> pa = Pattern.<TaxiFare>begin("first",AfterMatchSkipStrategy.skipToNext())
                .where(new SimpleCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare value) throws Exception {
                        return value.totalFare < 10.0F;
                    }
                })
                .oneOrMore()
                .followedBy("satisfyTotalFare")
                .where(new SimpleCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare value) throws Exception {
                        return value.totalFare > 20.0F;
                    }
                });

        CEP.pattern(source, pa)
                .process(new PatternProcessFunction<TaxiFare, List<Tuple3<Long, String, Float>>>() {
                    @Override
                    public void processMatch(Map<String, List<TaxiFare>> match, Context ctx, Collector<List<Tuple3<Long, String, Float>>> out) throws Exception {
                        List<TaxiFare> first = match.get("first");
                        TaxiFare satisfyTotalFare = match.get("satisfyTotalFare").iterator().next();
                        ArrayList<Tuple3<Long, String, Float>> list = new ArrayList<>();
                        for (TaxiFare taxiFare : first) {
                            list.add(new Tuple3<>(taxiFare.rideId, taxiFare.paymentType, taxiFare.totalFare));
                        }
                        list.add(new Tuple3<>(satisfyTotalFare.rideId, satisfyTotalFare.paymentType, satisfyTotalFare.totalFare));
                        out.collect(list);
                    }
                })
                .print();
    }

    @AfterAll
    public static void after() throws Exception {
        // Ensure the Flink job is executed
        env.execute();
    }
}
