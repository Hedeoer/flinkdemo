package cn.hedeoer.chaptor04;


import cn.hedeoer.common.datatypes.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class $03CEP_LatenessEvent {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<LoginEvent> stream = env
                .fromElements(
                        new LoginEvent("user_1", "0", "fail", 2000L),
                        new LoginEvent("user_1", "1", "fail", 3000L),
                        new LoginEvent("user_1", "2", "fail", 4000L),
                        new LoginEvent("user_1", "3", "fail", 5000L),
                        new LoginEvent("user_1", "4", "fail", 10000L),
                        new LoginEvent("user_1", "5", "fail", 11000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                                        return loginEvent.eventTime;
                                    }
                                })
                )
                .keyBy(r -> r.userId);

        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .within(Time.seconds(5));


        PatternStream<LoginEvent> patternedStream = CEP.pattern(stream, pattern);

        // 主流中定义测流
        OutputTag<String> outputTag = new OutputTag<String>("side-output") {
        };

        // 调用 flatSelect，使得处理超时的测流能够在主流中访问到
        SingleOutputStreamOperator<String> mainStream = patternedStream
                .flatSelect(outputTag, new MyPatternFlatTimeoutFunction(), new MyPatternFlatSelectFunction());

        mainStream.print();
        mainStream.getSideOutput(outputTag).printToErr();

        //user_1 0 1 2
        //user_1 1 2 3
        //user_1 2
        //user_1 3
        //user_1 4
        //user_1 5
        env.execute();
    }


    // 处理超时的
    public static class MyPatternFlatTimeoutFunction implements PatternFlatTimeoutFunction<LoginEvent, String> {
        @Override
        public void timeout(Map<String, List<LoginEvent>> map, long timeoutTimestamp, Collector<String> out) throws Exception {
            LoginEvent first = map.get("first").iterator().next();
            out.collect(first.userId + " " + first.ipAddress);
        }
    }
    // 处理匹配成功的
    public static class MyPatternFlatSelectFunction implements PatternFlatSelectFunction<LoginEvent, String> {
        @Override
        public void flatSelect(Map<String, List<LoginEvent>> map, Collector<String> out) throws Exception {
            LoginEvent first = map.get("first").iterator().next();
            LoginEvent second = map.get("second").iterator().next();
            LoginEvent third = map.get("third").iterator().next();
            StringBuilder builder = new StringBuilder();
            builder.append(first.userId)
                    .append(" ")
                    .append(first.ipAddress)
                    .append(" ")
                    .append(second.ipAddress)
                    .append(" ")
                    .append(third.ipAddress);

            out.collect(builder.toString());
        }
    }

}
