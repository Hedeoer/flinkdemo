package cn.hedeoer.chapter01.state;


//Keyed State 之MapState的使用

import cn.hedeoer.common.datatypes.Event;
import cn.hedeoer.common.sources.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

// keyState 之 mapstate

public class $05MapState {
    // 每个用户的页面对应的点击次数
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> source = env.addSource(new ClickSource());
        source
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp))
                .keyBy(data -> data.user)
                .process(new MyClickAnalysic())
                .print();

        env.execute();

    }

    public static class MyClickAnalysic extends ProcessFunction<Event,String>{

        private MapState<String, Long> userClick;

        @Override
        public void open(Configuration parameters) throws Exception {

            userClick = getRuntimeContext().getMapState(new MapStateDescriptor<>("user-click",String.class,Long.class));
        }

        @Override
        public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
            Long l = userClick.get(value.url);
            if (l == null) {
                userClick.put(value.url,1L);
            }else{

                userClick.put(value.url,l+1L);
            }

            Iterable<Map.Entry<String, Long>> entries = userClick.entries();
            for (Map.Entry<String, Long> entry : entries) {
                out.collect(value.user + " 点击了　" +entry.getKey() + ": " + entry.getValue() +"次");
            }
        }


    }

}
