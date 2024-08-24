package cn.hedeoer.chaptor04;

import cn.hedeoer.common.datatypes.TaxiFare;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * 1. cep并非包含在flink的二进制包中，如果在集群运行需要上传相应的jar处理
 * 2. 使用cep进行模式匹配，对pojo类型必须考虑hashcode和equals方法
 * 3. 涉及内容
 *  3.1 单个模式
 *  3.2 组合模式
 *  3.3 模式组
 *  3.4 匹配数据的处理
 *  3.5 cep中迟到数据的处理
 */
public class $01CEP_Individual {

    private static StreamExecutionEnvironment env;
    private static SingleOutputStreamOperator<TaxiFare> source;

    @BeforeAll
    public static void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        source = env.readTextFile("output/taxifare/2024-08-24--19/.part-0-0.inprogress.bcdffd45-8338-4218-ab86-4cd998c2dfc4")
                .map(line -> {
                    String[] contents = line.split("\t");
                    return new TaxiFare(Long.parseLong(contents[0]),
                            Long.parseLong(contents[1]),
                            Long.parseLong(contents[2]),
                            Instant.parse(contents[3]),
                            contents[4],
                            Float.parseFloat(contents[5]),
                            Float.parseFloat(contents[6]),
                            Float.parseFloat(contents[7]),
                            contents[8]);
                });
    }

    @Test
    public void simpleCondition() throws Exception {

        // 1. 创建模式
        Pattern<TaxiFare, TaxiFare> pattern = Pattern.<TaxiFare>begin("cashStream")
                .where(new IterativeCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare taxiFare, Context<TaxiFare> context) throws Exception {
                        return taxiFare.paymentType.equals("CASH");
                    }
                });

        // 为数据流应用模式
        PatternStream<TaxiFare> patternStream = CEP.pattern(source, pattern);
        // 处理模式匹配的结果
        patternStream.process(new PatternProcessFunction<TaxiFare, TaxiFare>() {
            @Override
            public void processMatch(Map<String, List<TaxiFare>> fares, Context context, Collector<TaxiFare> collector) throws Exception {
                for (TaxiFare taxiFare : fares.get("cashStream")) {
                    collector.collect(taxiFare);
                }
            }
        }).print();




    }

    @AfterAll
    public static void after() throws Exception {
        env.execute();
    }
}
