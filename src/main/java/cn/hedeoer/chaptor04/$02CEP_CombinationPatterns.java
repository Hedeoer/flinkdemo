package cn.hedeoer.chaptor04;

import cn.hedeoer.common.datatypes.TaxiFare;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

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
public class $02CEP_CombinationPatterns {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment);
        SingleOutputStreamOperator<TaxiFare> source = executionEnvironment.readTextFile("output/taxifare/2024-08-24--19/.part-0-0.inprogress.bcdffd45-8338-4218-ab86-4cd998c2dfc4")
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


        Pattern<TaxiFare, TaxiFare> pattern = Pattern.<TaxiFare>begin("begin")
                .where(new IterativeCondition<TaxiFare>() {
                    @Override
                    public boolean filter(TaxiFare value, Context<TaxiFare> ctx) throws Exception {
                        return "CASH".equals(value.paymentType);
                    }
                });

        PatternStream<TaxiFare> patternStream = CEP.pattern(source, pattern);
        patternStream.process(new PatternProcessFunction<TaxiFare, String>() {
            @Override
            public void processMatch(Map<String, List<TaxiFare>> match, Context ctx, Collector<String> out) throws Exception {
                for (TaxiFare begin : match.get("begin")) {
                    out.collect(begin.toString());
                }
            }
        }).print();
    }
}
