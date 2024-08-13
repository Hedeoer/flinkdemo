package cn.hedeoer.chapter01.state;

import cn.hedeoer.common.datatypes.Item;
import cn.hedeoer.common.datatypes.Rule;
import cn.hedeoer.common.datatypes.Shape;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Broadcast State：
 * 1. 下游算子实例访问相同的一份广播状态
 * 2. 下游算子在扩容后，只需复制或者清理一份广播状态多次即可完成对所需的广播状态的访问
 * 3. 使用场景：流量低的数据流中包含了一些规则，而其他流需要根据规则进行一些处理
 * 4. Broadcast State有类似map的结构
 *
 */
public class $07OperateState_BroadCastState {
    // 找出相同颜色的元素流中符合规则的item对
    // 规则1：圆形 -> 方形
    // 规则2：三角形 -> 圆形
    public static void main(String[] args) throws Exception {

        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟Item数据流
        DataStream<Item> itemStream = env.addSource(new SourceFunction<Item>() {
            @Override
            public void run(SourceContext<Item> ctx) throws Exception {
                Thread.sleep(1000);
                ctx.collect(new Item("Item1", Shape.CIRCLE,"ORANGE"));    // 圆形
                Thread.sleep(1000);
                ctx.collect(new Item("Item2", Shape.SQUARE,"ORANGE"));    // 方形
                Thread.sleep(1000);
                ctx.collect(new Item("Item3", Shape.TRIANGLE,"ORANGE"));  // 三角形
                Thread.sleep(1000);
                ctx.collect(new Item("Item4", Shape.CIRCLE,"ORANGE"));    // 圆形
                Thread.sleep(1000);
                ctx.collect(new Item("Item5", Shape.SQUARE,"ORANGE"));    // 圆形
            }

            @Override
            public void cancel() {
            }
        });

        // 模拟Rule广播数据流
        List<Rule> rules = Arrays.asList(
                new Rule("R1", Shape.CIRCLE, Shape.SQUARE),    // 规则1：圆形 -> 方形
                new Rule("R2", Shape.TRIANGLE, Shape.CIRCLE)   // 规则2：三角形 -> 圆形
        );
        // 规则流
        DataStream<Rule> ruleStream = env.fromCollection(rules);
        // BroadcastState就是map结构存储的
        MapStateDescriptor<String, Rule> rulesMap = new MapStateDescriptor<>("rules", String.class, Rule.class);
        // 广播流
        BroadcastStream<Rule> broadcast = ruleStream.broadcast(rulesMap);

        itemStream
                // 按照元素中的颜色分组
                .keyBy(Item::getColor)
                // 链接广播流，s1.connect(broadcast),调用的顺寻不能变
                .connect(broadcast)
                // KeyedBroadcastProcessFunction处理链接后的流，进行自己的规则匹配逻辑处理
                .process(new PatternShape())
                .print();

        env.execute();

    }

    public static class PatternShape extends KeyedBroadcastProcessFunction<String, Item, Rule, String> {


        private   ValueState<Item> preItem;



        // 初始化执行一次
        @Override
        public void open(Configuration parameters) throws Exception {
            preItem = getRuntimeContext().getState(new ValueStateDescriptor<Item>("preItem", Item.class));

        }

        /**
         * 数据流中来一个元素调用一次本方法
         * 如果需要定义定时器，只能在processElement定义
         * @param value The stream element.
         * @param ctx A {@link ReadOnlyContext} that allows querying the timestamp of the element,
         *     querying the current processing/event time and iterating the broadcast state with
         *     <b>read-only</b> access. The context is only valid during the invocation of this method,
         *     do not store it.
         * @param out The collector to emit resulting elements to
         * @throws Exception
         */
        @Override
        public void processElement(Item value, KeyedBroadcastProcessFunction<String, Item, Rule, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
            // 获取存储规则的广播
            ReadOnlyBroadcastState<String, Rule> mapState = ctx.getBroadcastState(new MapStateDescriptor<>("rules", String.class, Rule.class));

            // 获取前一个元素
            Item item = preItem.value();

            // 更新 preItem 为当前元素
            preItem.update(value);


            // 如果广播不为空，并且前一个元素不为空，则进行规则匹配
            if (mapState != null && item != null) {
                Iterable<Map.Entry<String, Rule>> entries = mapState.immutableEntries();
                Iterator<Map.Entry<String, Rule>> iterator = entries.iterator();

                while (iterator.hasNext()) {
                    Map.Entry<String, Rule> ruleEntry = iterator.next();
                    Rule rule = ruleEntry.getValue();

                    Shape first = rule.first;
                    Shape second = rule.second;
                    System.out.println(rule);

                    // 检查前一个元素和当前元素是否符合规则
                    if (item.getShape().equals(first) && value.getShape().equals(second)) {
                        out.collect(item + " ==> " + value  + " match " + rule );
                    }
                }
            }
        }

        @Override
        public void processBroadcastElement(Rule value, KeyedBroadcastProcessFunction<String, Item, Rule, String>.Context ctx, Collector<String> out) throws Exception {
            BroadcastState<String, Rule> broadcastState = ctx.getBroadcastState( new MapStateDescriptor<>("rules", String.class, Rule.class));
            // 规则名字为key，value本身为value，放入广播状态中
            broadcastState.put(value.name, value);
        }

        /**
         * KeyedBroadcastProcessFunction 相比于 BroadcastProcessFunction  多提供的了定时器的功能， 但是定时器的定义”只能“在 processElement方法 中
         * @param timestamp The timestamp of the firing timer.
         * @param ctx An {@link OnTimerContext} that allows querying the timestamp of the firing timer,
         *     querying the current processing/event time, iterating the broadcast state with
         *     <b>read-only</b> access, querying the {@link TimeDomain} of the firing timer and getting
         *     a {@link TimerService} for registering timers and querying the time. The context is only
         *     valid during the invocation of this method, do not store it.
         * @param out The collector for returning result values.
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, KeyedBroadcastProcessFunction<String, Item, Rule, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }
    }
}
