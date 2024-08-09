package cn.hedeoer.chapter01.operations.datastream_transformations.evictors;


import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;
import java.util.Objects;

/**
 * flink evictors 的使用
 * 调用时机的三种情况：
 * ①触发器（trigger）触发（fire）之前
 * ②窗口函数计算之前
 * ③窗口函数计算之后
 *
 * window算子编程流程：
 * stream
 *        .keyBy(...)               <-  keyed versus non-keyed windows
 *        .window(...)              <-  required: "assigner"
 *       [.trigger(...)]            <-  optional: "trigger" (else default trigger)
 *       [.evictor(...)]            <-  optional: "evictor" (else no evictor)
 *       [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
 *       [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
 *        .reduce/aggregate/apply()      <-  required: "function"
 *       [.getSideOutput(...)]      <-  optional: "output tag
 *
 * 或者
 *
 * stream
 *        .windowAll(...)           <-  required: "assigner"
 *       [.trigger(...)]            <-  optional: "trigger" (else default trigger)
 *       [.evictor(...)]            <-  optional: "evictor" (else no evictor)
 *       [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
 *       [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
 *        .reduce/aggregate/apply()      <-  required: "function"
 *       [.getSideOutput(...)]      <-  optional: "output tag"
 *
 *
 */
public class $13EvictorTest {
    /**
     * 演示窗口函数计算之后的情况：
     * 每30秒计算每个司机的过去30秒内的最高的单笔收入，如果单笔收入不超过100元，直接清理；超过100元，在窗口打印；
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiFare> source = env.addSource(new TaxiFareGenerator());/*.addSink(TaxiRideSink.taxiRideSink());*/

        source
        .keyBy(fare -> fare.driverId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .evictor(new MyEvictor())
                .reduce((fare1, fare2) -> fare1.totalFare > fare2.totalFare ? fare1 : fare2)
                .filter(Objects::nonNull)
                .print()
                .setParallelism(1);

        env.execute();

    }

    public static class MyEvictor implements Evictor<TaxiFare, TimeWindow>{

        // 窗口触发之前执行
        @Override
        public void evictBefore(Iterable<TimestampedValue<TaxiFare>> elements,
                                int size,
                                TimeWindow window,
                                EvictorContext evictorContext) {

            long counts = 0;
            Iterator<TimestampedValue<TaxiFare>> iterator = elements.iterator();
            while (iterator.hasNext()) {
                TaxiFare fare = iterator.next().getValue();
                // 删除小于100元的
                if (fare.totalFare < 100f) {
                    // 对删除的记录数做一个统计输出
                    counts ++;
                    iterator.remove();
                }
            }

            System.out.println("deleteCounts: " + counts);

        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<TaxiFare>> elements,
                               int size,
                               TimeWindow window,
                               EvictorContext evictorContext) {






        }
    }
}
