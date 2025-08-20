package cn.hedeoer.chaptor06;

import cn.hedeoer.common.datatypes.ValueEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * sessionWindowJoin
 * 1. 在指定的会话窗口内对满足join条件的数据进行join
 */
public class $03SessionWindowJoinByDataStream {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2. 创建 greenStream，监听 8888 端口
        DataStream<ValueEvent> greenStream = env.socketTextStream("localhost", 8888)
                .map(new $02TumblingWindowJoinByDataStream.StringToEventMap() )
                // 分配时间戳和 Watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ValueEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((valueEvent, recordTimestamp) -> valueEvent.getTimestamp())
                );

        // 3. 创建 orangeStream，监听 9999 端口
        DataStream<ValueEvent> orangeStream = env.socketTextStream("localhost", 9999)
                .map(new $02TumblingWindowJoinByDataStream.StringToEventMap() )
                // 分配时间戳和 Watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ValueEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((valueEvent, recordTimestamp) -> valueEvent.getTimestamp())
                );


        // 4. 实现 Window Join
        DataStream<String> joinedStream = greenStream.join(orangeStream)
                // a. 指定第一个流的 key
                .where(valueEvent -> valueEvent.getKey())
                // b. 指定第二个流的 key
                .equalTo(valueEvent -> valueEvent.getKey())
                // c. 定义一个5秒的会话时间窗口
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                // d. 提供一个 JoinFunction 来处理匹配的元素对
                .apply(new JoinFunction<ValueEvent, ValueEvent, String>() {
                    @Override
                    public String join(ValueEvent greenEvent, ValueEvent orangeEvent) throws Exception {
                        return "JOIN SUCCESS! -> " + greenEvent.toString() + " <-> " + orangeEvent.toString();
                    }
                });

        // 5. 打印结果
        joinedStream.print();
/*
输入：
PS C:\Users\H> nc -l -p 8888
1,2025-08-20 12:00:20
1,2025-08-20 12:00:21
1,2025-08-20 12:00:26
1,2025-08-20 12:00:32

PS C:\Users\H>  nc -l -p 9999
1,2025-08-20 12:00:20
1,2025-08-20 12:00:21
1,2025-08-20 12:00:26
1,2025-08-20 12:00:32

输出：
JOIN SUCCESS! -> Event{key=1, time=12:00:20} <-> Event{key=1, time=12:00:20}
JOIN SUCCESS! -> Event{key=1, time=12:00:20} <-> Event{key=1, time=12:00:21}
JOIN SUCCESS! -> Event{key=1, time=12:00:20} <-> Event{key=1, time=12:00:26}
JOIN SUCCESS! -> Event{key=1, time=12:00:21} <-> Event{key=1, time=12:00:20}
JOIN SUCCESS! -> Event{key=1, time=12:00:21} <-> Event{key=1, time=12:00:21}
JOIN SUCCESS! -> Event{key=1, time=12:00:21} <-> Event{key=1, time=12:00:26}
JOIN SUCCESS! -> Event{key=1, time=12:00:26} <-> Event{key=1, time=12:00:20}
JOIN SUCCESS! -> Event{key=1, time=12:00:26} <-> Event{key=1, time=12:00:21}
JOIN SUCCESS! -> Event{key=1, time=12:00:26} <-> Event{key=1, time=12:00:26}

* */

        // 执行作业
        env.execute("Session Window Join Example");
    }

    public static class StringToEventMap extends RichMapFunction<String, ValueEvent> {
        // 声明一个成员变量
        private transient DateTimeFormatter formatter;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 在 open 方法中初始化，这个方法在每个 TaskManager 的任务实例中只执行一次
            super.open(parameters);
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public ValueEvent map(String value) throws Exception {
            String[] parts = value.split(",");
            int key = Integer.parseInt(parts[0].trim());
            LocalDateTime ldt = LocalDateTime.parse(parts[1].trim(), formatter);
            long timestamp = ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            return new ValueEvent(key, timestamp);
        }
    }
}
