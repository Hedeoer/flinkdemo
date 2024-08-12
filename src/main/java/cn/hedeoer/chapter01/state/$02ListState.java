package cn.hedeoer.chapter01.state;

import cn.hedeoer.common.datatypes.Event;
import cn.hedeoer.common.sources.ClickSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// Keyed State 之ListState
public class $02ListState {

    // 在用户的点击流查询用户每个点击5次后，返回用户的首次点击链接，和最后一次点击链接tuple2<user_id, first_url,last_url>
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> source = env.addSource(new ClickSource());
        // 统计每个用户的点击次数,每三次输出一次
        source.keyBy(data -> data.user)
                .flatMap(new MyMapFunction())
                .print();
        env.execute();
    }

    public static class MyMapFunction extends RichFlatMapFunction<Event, String> {


        private ListState<Event> eventList;
        // 用户的点击次数
        private Long counts;

        @Override
        public void open(Configuration parameters) throws Exception {
            counts = 0L;
            eventList = getRuntimeContext().getListState(new ListStateDescriptor<Event>("eventList", Event.class));
        }


        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {

            eventList.add(value);
            counts++;



            if (counts % 5 ==0) {
                Iterable<Event> events = eventList.get();
                // 需要注意java Stream中的流的不可复用性
                // 用户的首次点击链接
                Event first = StreamSupport.stream(events.spliterator(), false).
                        min(Comparator.comparingLong(e -> e.timestamp)).orElse(null);

                // 用户的最后一次点击链接
                List<Event> collect = StreamSupport.stream(events.spliterator(), false)
                        .sorted(Comparator.comparingLong(e -> e.timestamp))
                        .collect(Collectors.toList());
                Event last = collect.isEmpty() ? null : collect.get(collect.size() - 1);

                assert first != null;
                assert last != null;
                out.collect(value.user + ": " + first.url + " ==> " + last.url);

                // 满5次清理List状态
                eventList.clear();
            }

        }
    }
}
