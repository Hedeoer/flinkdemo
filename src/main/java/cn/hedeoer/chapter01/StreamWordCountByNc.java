package cn.hedeoer.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCountByNc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("hadoop102", 8989)
                        .flatMap(new FlatMapFunction<String, String>() {
                            @Override
                            public void flatMap(String s, Collector<String> collector) throws Exception {
                                for (String str : s.split(" ")) {
                                    collector.collect(str);
                                }
                            }
                        })
                                .map(word -> Tuple2.of(word, 1))
                                        .returns(Types.TUPLE(Types.STRING, Types.INT))
                                                .keyBy(data -> data.f0)
                                                        .sum(1)
                                                                .print();

        env.execute();
    }
}
