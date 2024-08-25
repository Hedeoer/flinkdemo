package cn.hedeoer.chaptor04;

import cn.hedeoer.common.datatypes.TaxiFare;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
 * 1. cep并非包含在flink的二进制包中，如果在集群运行需要上传相应的jar处理
 * 2. 使用cep进行模式匹配，对pojo类型必须考虑hashcode和equals方法
 * 3. 涉及内容
 * 3.1 单个模式
 * 3.2 组合模式
 *      对应循环模式：比如times（）， oneOrMore(), timesOrMore(),默认是松散连续（即followBy的效果）；如果要使用严格连续，需要使用consecutive()方法；
 * 3.3 模式组
 * 3.4 匹配数据的处理
 * 3.5 cep中迟到数据的处理
 */
public class $01CEP_Individual {

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
     * 1. 简单条件匹配
     * 支付方式为CASH的记录
     *
     * @throws Exception
     */
    @Test
    public void simpleCondition() throws Exception {
        // 1. 创建模式
        Pattern<TaxiFare, TaxiFare> pattern = Pattern.<TaxiFare>begin("cashStream")
                .where(new SimpleCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare value) throws Exception {
                        return value.paymentType.equals("CASH");
                    }
                });

        // 为数据流应用模式
        PatternStream<TaxiFare> patternStream = CEP.pattern(source, pattern);

        // 处理模式匹配的结果
        patternStream.select(new PatternSelectFunction<TaxiFare, List<TaxiFare>>() {
            @Override
            public List<TaxiFare> select(Map<String, List<TaxiFare>> map) throws Exception {
                return map.get("cashStream");
            }
        }).print();
        //[1,2013000185,2013000185,2020-01-01T12:00:20Z,CASH,33.0,0.0,118.0,JPY]
        //[3,2013000134,2013000134,2020-01-01T12:01:00Z,CASH,12.0,0.0,41.0,JPY]
        //[5,2013000087,2013000087,2020-01-01T12:01:40Z,CASH,14.0,0.0,46.0,JPY]
        //[7,2013000036,2013000036,2020-01-01T12:02:20Z,CASH,23.0,0.0,80.0,JPY]
        // .....

        // Execute the Flink job
        env.execute("Flink CEP Example");
    }

    /**
     * 量词
     * times(),
     * optional()
     * oneOrMore()
     * timesOrMore()
     * greedy()
     * 过滤出每个司机过路费连续免费的情况（tolls = 0.0）
     * 如果出现收费中断的情况，后续还有连续2次免费的情况也要考虑在内
     */
    @Test
    public void QuantifiersPattern() {
        KeyedStream<TaxiFare, Long> keyedStream = source.keyBy(fare -> fare.driverId);

        // 匹配后的跳过策略
        // 本例中取 'start' 策略开头和 "end" 策略结尾的所有命中数据集中 取 匹配数量最多的数据集
        AfterMatchSkipStrategy strategy = AfterMatchSkipStrategy.skipPastLastEvent();

        Pattern<TaxiFare, TaxiFare> freeRides = Pattern.<TaxiFare>begin("start", strategy)
                // 过路费免费的情况
                .where(new SimpleCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare value) throws Exception {
                        return Float.parseFloat("0.0") == value.tolls;
                    }
                })
                // 2次及以上
                .timesOrMore(2)
                // 严格连续
                .consecutive()
                // 匹配结束为过路费不免费，非紧邻连续
                .followedBy("end")
                .where(new SimpleCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare value) throws Exception {
                        return value.tolls > Float.parseFloat("0.0");
                    }
                });


        CEP.pattern(keyedStream, freeRides)
                .process(new PatternProcessFunction<TaxiFare, List<Tuple3<Long, Long, Float>>>() {
                    @Override
                    public void processMatch(Map<String, List<TaxiFare>> match, Context ctx, Collector<List<Tuple3<Long, Long, Float>>> out) throws Exception {
                        // 取连续的数据即可，不需要匹配end策略的
                        List<TaxiFare> start = match.get("start");
                        ArrayList<Tuple3<Long, Long, Float>> array = new ArrayList<>();
                        for (TaxiFare taxiFare : start) {
                            array.add(new Tuple3<>(taxiFare.driverId, taxiFare.rideId, taxiFare.tolls));
                        }
                        out.collect(array);
                    }
                })
                .print();
        //[(2013000185,715,0.0), (2013000185,996,0.0), (2013000185,1244,0.0), (2013000185,1454,0.0), (2013000185,1529,0.0), (2013000185,1637,0.0)]
        //[(2013000014,673,0.0), (2013000014,962,0.0)]
        //[(2013000065,253,0.0), (2013000065,286,0.0), (2013000065,463,0.0), (2013000065,679,0.0)]
        //[(2013000065,1208,0.0), (2013000065,1385,0.0), (2013000065,1418,0.0), (2013000065,1601,0.0)]
        //[(2013000065,1951,0.0), (2013000065,2155,0.0)]

    }

    /**
     * 模式组使用
     * next(),followBy(),followByAny()
     * 第一次使用现金支付，第二次使用信用卡支付，并且第三次收款金额大于50的情况
     */
    @Test
    public void patternGroup() {

        // 模式1
        Pattern<TaxiFare, TaxiFare> cashThenCard = Pattern.<TaxiFare>begin("cashStart")
                .where(new SimpleCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare value) throws Exception {
                        return "CASH".equals(value.paymentType);
                    }
                })
                .next("cardEnd")
                .where(new SimpleCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare value) throws Exception {
                        return "CARD".equals(value.paymentType);
                    }
                });
        // 模式2
        Pattern<TaxiFare, TaxiFare> enoughTotalFare = Pattern.<TaxiFare>begin("enoughTotalFare")
                .where(new SimpleCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare value) throws Exception {
                        return value.totalFare > 50;
                    }
                });
        // 组成模式组
        GroupPattern<TaxiFare, TaxiFare> groupPattern = cashThenCard.next(enoughTotalFare);

        // 应用模式组
        PatternStream<TaxiFare> patternStream = CEP.pattern(source, groupPattern);

        patternStream.select(new PatternSelectFunction<TaxiFare, List<Tuple3<Long, Long, Long>>>() {
                    @Override
                    public List<Tuple3<Long, Long, Long>> select(Map<String, List<TaxiFare>> pattern) throws Exception {
                        ArrayList<Tuple3<Long, Long, Long>> tuple3s = new ArrayList<>();
                        List<TaxiFare> cashStart = pattern.get("cashStart");
                        List<TaxiFare> cardEnd = pattern.get("cardEnd");
                        List<TaxiFare> enoughTotalFare = pattern.get("enoughTotalFare");
                        Tuple3 tuple3 = new Tuple3(cashStart.get(0).rideId, cardEnd.get(0).rideId, enoughTotalFare.get(0).rideId);
                        tuple3s.add(tuple3);
                        return tuple3s;
                    }
                })
                .print();
        //[(5,6,7)]
        //[(7,8,9)]
        //[(9,10,11)]
        //[(11,12,13)]
        //[(15,16,17)]
    }

    @AfterAll
    public static void after() throws Exception {
        // Ensure the Flink job is executed
        env.execute();
    }
}
