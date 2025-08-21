package cn.hedeoer.chaptor06;

import cn.hedeoer.common.datatypes.Comment;
import cn.hedeoer.common.datatypes.Post;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/*
* left join
* 使用datastream api 实现 left join的效果
*
* */
public class $05LeftJoinDealWithPostCommentByDataStream {
    public static void main(String[] args) throws Exception {
       /*
       * 1. 定义java实体
       * 2. 定义水印生成策略
       * 3. 读取kafka数据
       * 4. connect
       * 5. coprocess处理
       * */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> postSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop103:9092")
                // 主题或者分区的订阅，可以同时订阅多个主题，也可以只订阅一个主题的多个分区
                .setTopics("mz_datagen_blog_posts")
                // 动态的分区发现，可以在主题下的分区增加时，自动发现
                .setProperty("partition.discovery.interval.ms", "10000")
                // 指定一个消费者组
                .setGroupId("group1")
                // 消费的方式
                .setStartingOffsets(OffsetsInitializer.latest())
                // 反序列化的方式
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> commentSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop103:9092")
                // 主题或者分区的订阅，可以同时订阅多个主题，也可以只订阅一个主题的多个分区
                .setTopics("mz_datagen_blog_comments")
                // 动态的分区发现，可以在主题下的分区增加时，自动发现
                .setProperty("partition.discovery.interval.ms", "10000")
                // 指定一个消费者组
                .setGroupId("group2")
                // 消费的方式
                .setStartingOffsets(OffsetsInitializer.latest())
                // 反序列化的方式
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 提取kafka元数据的timestamp作为事件时间戳并分配水印
        WatermarkStrategy<String> postStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((element, recordTimestamp) ->recordTimestamp)
                // 此处flink测试环境的并行度为24，kafka主题 mz_datagen_blog_posts 有1个分区
                .withIdleness(Duration.ofSeconds(5));

        // 提取kafka元数据的timestamp作为事件时间戳并分配水印
        WatermarkStrategy<String> commentStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((element, recordTimestamp) ->recordTimestamp)
                // 此处flink测试环境的并行度为24，kafka主题 mz_datagen_blog_posts 有1个分区
                .withIdleness(Duration.ofSeconds(5));

        SingleOutputStreamOperator<Post> post = env.fromSource(postSource, postStrategy, "post")
                .map(new MyPostMappingFunction());

        SingleOutputStreamOperator<Comment> comment = env.fromSource(commentSource, commentStrategy, "comment")
                .map(new MyCommentMappingFunction());

        // connect
        post.connect(comment)
                .keyBy(Post::getId, Comment::getPostId)
                        .process(new MyKeyProcessFunctions())
                                .map(new MyFilterMapFunction())
                                        .print();


        env.execute();


    }

    private static class MyPostMappingFunction implements MapFunction<String, Post> {
        @Override
        public Post map(String value) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
            mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return mapper.readValue(value, Post.class);
        }
    }

    private static class MyCommentMappingFunction implements MapFunction<String, Comment>{
        @Override
        public Comment map(String value) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
            return mapper.readValue(value, Comment.class);
        }
    }


    /**
     * CoProcessFunction实现left join的效果
     * select
     *     t1.id as post_id,
     *     t1.title as post_title,
     *     t2.id as comment_id,
     *     t2.body as comment_body
     * from posts t1
     * left join comments t2
     * on t1.id = t2.post_id
     *
     * 初始化：
     * 创建 postValueStatue 存储所有到达的post数据
     * 创建 commentListStatue 存储所有到达的comment数据
     *
     *
     *对post数据的处理：
     * 为该时间点创建一个事件时间定时器（5秒）
     * 存储该post数据到postValueStatue
     * 查询comment数据的情况：
     * 1. 如果commentListStatue为空，则输出一条（post，null）的数据
     * 2. 如果commentListStatue不为空，则遍历commentListStatue，将post数据与comment数据进行关联，并输出；且删除之前的事件时间定时器，重新设置一个事件时间定时器（5秒）
     *
     * 对于comment数据的处理：
     * 为该时间点创建一个事件时间定时器（5秒）
     * 存储comment数据到commentListStatue
     * 查询post数据情况：
     * 1. 如果postValueStatue为空，不做任何处理
     * 2. 如果postValueStatue不为空，将comment数据与post数据进行关联，并输出一条（post，comment）数据，并删除之前的事件时间定时器，创建一个5秒后的事件时间定时器
     *
     * 定时器的触发：表示5秒定时已到
     * 清理postValueStatue
     * 清理commentListStatue
     *
     *
     *
     */
    private static class MyKeyProcessFunctions extends KeyedCoProcessFunction<Long, Post, Comment, Tuple2<Post, Comment>>{

        private ListState<Comment> commentListStates;
        private ValueState<Post> postValueState;
        private ValueState<Long> previousTimerFireTimeValueStatue;

        @Override
        public void open(Configuration parameters) throws Exception {
            postValueState = getRuntimeContext().getState(new ValueStateDescriptor<Post>("postValueState", Post.class));
            commentListStates = getRuntimeContext().getListState(new ListStateDescriptor<Comment>("commentListStates", Comment.class));
            previousTimerFireTimeValueStatue = getRuntimeContext().getState(new ValueStateDescriptor<>("previousTimerFireTimeValueStatue", Long.class));

        }

        @Override
        public void processElement1(Post value, KeyedCoProcessFunction<Long, Post, Comment, Tuple2<Post, Comment>>.Context ctx, Collector<Tuple2<Post, Comment>> out) throws Exception {
            postValueState.update( value);

            boolean hasJoined = false;
            boolean needDeleteTimer = false;
            for (Comment comment : commentListStates.get()) {
                out.collect(Tuple2.of(value, comment));
                if (comment != null) {
                    needDeleteTimer = true;
                    hasJoined = true;
                }
            }

            if (!hasJoined) {
                out.collect(Tuple2.of(value, null));
            }

            Long planFireTime = previousTimerFireTimeValueStatue.value();
            if (needDeleteTimer && planFireTime != null) {
                ctx.timerService().deleteEventTimeTimer(planFireTime);
            }

            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5 * 1000L);
            previousTimerFireTimeValueStatue.update(ctx.timestamp() + 5 * 1000L);

        }

        @Override
        public void processElement2(Comment value, KeyedCoProcessFunction<Long, Post, Comment, Tuple2<Post, Comment>>.Context ctx, Collector<Tuple2<Post, Comment>> out) throws Exception {

            commentListStates.add(value);

            if (postValueState.value() != null) {
                out.collect(Tuple2.of(postValueState.value(), value));
                Long planFireTime = previousTimerFireTimeValueStatue.value();
                if (planFireTime != null) {
                    ctx.timerService().deleteEventTimeTimer(planFireTime);
                }
            }
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5 * 1000L);
            previousTimerFireTimeValueStatue.update(ctx.timestamp() + 5 * 1000L);


        }

        @Override
        public void onTimer(long timestamp, KeyedCoProcessFunction<Long, Post, Comment, Tuple2<Post, Comment>>.OnTimerContext ctx, Collector<Tuple2<Post, Comment>> out) throws Exception {
            if (previousTimerFireTimeValueStatue.value() != null && timestamp == previousTimerFireTimeValueStatue.value() ) {
                postValueState.clear();
                commentListStates.clear();
                previousTimerFireTimeValueStatue.clear();
            }
        }
    }

    private static class MyFilterMapFunction implements MapFunction<Tuple2<Post, Comment>, String> {
        @Override
        public String map(Tuple2<Post, Comment> value) throws Exception {
            Long postId = value.f0.getId();
            String title = "" ;
            Long commentId = value.f1 == null ? 0L : value.f1.getId();
            String comment_body = "";
            return postId + " :==> " + title + " :==> " + commentId + " :==> " + comment_body;
        }
    }
}
