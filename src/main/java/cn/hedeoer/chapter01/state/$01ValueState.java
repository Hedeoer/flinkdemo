package cn.hedeoer.chapter01.state;

import cn.hedeoer.common.datatypes.Event;
import cn.hedeoer.common.sources.ClickSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// keyState 之valueState
public class $01ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> source = env.addSource(new ClickSource());
        // 统计每个用户的点击次数,每三次输出一次
        source.keyBy(data -> data.user)
                .flatMap(new MyFlatMapFunction())
                .print();
        env.execute();
    }

    public static class MyFlatMapFunction extends RichFlatMapFunction<Event, Tuple2<String, Long>> {

        // 每个用户的点击次数
        private transient ValueState<Long> countTimes;

        @Override
        public void open(Configuration parameters) throws Exception {
            countTimes = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("countTimes", TypeInformation.of(Long.class)));
        }

        @Override
        public void flatMap(Event value, Collector<Tuple2<String, Long>> out) throws Exception {
            Long counts = countTimes.value();
            if (counts == null) {
                countTimes.update(1L);
            }else{
                countTimes.update(counts + 1);
                if(counts == 3){
                    out.collect(Tuple2.of(value.user, counts));
                    countTimes.clear();
                }
            }

        }


    }
}
