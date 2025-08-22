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
import org.apache.flink.streaming.api.TimeDomain;
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
                        .process(new PostCommentLeftJoinFunction())
                                .map(new MyFilterMapFunction())
                                        .print();

/*
演示思路：
输入为：
post主题：
{"user_id":14,"id":151,"title":"First post for key 151.","body":"..."}
{"user_id":20,"id":200,"title":"Some other post.","body":"..."}

输入comment主题：
{"post_id":151,"id":980,"user_id":69,"body":"First comment for post 151."}
{"post_id":151,"id":982,"user_id":70,"body":"Second comment for post 151."}

等待20秒。

输入post主题：
{"user_id":30,"id":300,"title":"A final post to advance watermark.","body":"..."}
{"user_id":14,"id":151,"title":"The same post 151, arriving AGAIN.","body":"..."}

结果：
151 :==>  :==> 0 :==>   post主题的元素先来，此时comment主题的元素还没有来，此时left join的结果为空
200 :==>  :==> 0 :==>   post主题的元素先来，此时comment主题的元素还没有来，此时left join的结果为空
151 :==>  :==> 980 :==> comment主题的元素来了，此时left join有可关联的值
151 :==>  :==> 982 :==> comment主题的元素来了，此时left join有可关联的值
300 :==>  :==> 0 :==>   经过20秒，此时post主题到来新元素，右表为空
151 :==>  :==> 0 :==>   经过20秒，此时post主题到来151的旧数据，此时由于超过了20秒限制，comment中 980，982的元素都已经过期清理了，此时left join结果为空



* */
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
     * 使用 KeyedCoProcessFunction 实现流的 LEFT JOIN。
     * <p>
     * 目标SQL:
     * SELECT
     *     t1.id as post_id,
     *     t1.title as post_title,
     *     t2.id as comment_id,
     *     t2.body as comment_body
     * FROM posts t1
     * LEFT JOIN comments t2
     * ON t1.id = t2.post_id
     * <p>
     * 实现思路:
     * 1. 使用 Flink 的 keyed state 来缓存先进来的数据 (可能是 Post 或 Comment)。
     * 2. 当左表 (Post) 的数据到达时，它必须被输出。如果此时右表 (Comment) 的数据已经存在于状态中，则进行 join；否则，输出 (post, null)。
     * 3. 当右表 (Comment) 的数据到达时，如果左表 (Post) 的数据已经存在，则进行 join；否则，仅将 Comment 存入状态等待 Post。
     * 4. 使用事件时间定时器来自动清理长时间没有新数据到达的 key 所对应的状态，防止状态无限增长。
     */
    private static class PostCommentLeftJoinFunction extends KeyedCoProcessFunction<Long, Post, Comment, Tuple2<Post, Comment>> {

        // 缓存 Post
        private ValueState<Post> postValueState;
        // 缓存 Comment
        private ListState<Comment> commentListState;
        // 存储定时器触发的时间戳
        private ValueState<Long> cleanupTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            postValueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("postValueState", Post.class));
            commentListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("commentListStates", Comment.class));
            cleanupTimerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("cleanupTimerState", Long.class));
        }

        /**
         * 处理 Post (左流)
         */
        @Override
        public void processElement1(Post post, Context ctx, Collector<Tuple2<Post, Comment>> out) throws Exception {
            // 1. 更新 Post 状态
            postValueState.update(post);

            // 2. 检查是否有已缓存的 Comment
            Iterable<Comment> comments = commentListState.get();
            if (comments == null || !comments.iterator().hasNext()) {
                    out.collect(Tuple2.of(post, null));
            } else {
                // 2b. 如果有，将 post 与每一个已缓存的 comment 进行 join
                for (Comment comment : comments) {
                    out.collect(Tuple2.of(post, comment));
                }
            }

            // 3. 重置定时器
            resetCleanupTimer(ctx);
        }

        /**
         * 处理 Comment (右流)
         */
        @Override
        public void processElement2(Comment comment, Context ctx, Collector<Tuple2<Post, Comment>> out) throws Exception {
            // 1. 无论如何，都将 Comment 存入列表状态
            commentListState.add(comment);

            // 2. 检查 Post 是否已经到达
            Post post = postValueState.value();
            if (post != null) {
                // 2a. 如果 Post 已存在，输出 join 结果
                out.collect(Tuple2.of(post, comment));
            }
            // 2b. 如果 Post 不存在，则不输出，等待 Post 到达。

            // 3. 重置定时器
            resetCleanupTimer(ctx);
        }

        /**
         *
         * postValueState：存储 Post 的状态。
         * commentListState：存储 Comment 的状态。
         * cleanupTimerState：存储定时器的触发时间。
         *
         * 触发条件：20秒内没有新数据到达的 key 所对应的状态，由于相同的key共享同一份（postValueState commentListState cleanupTimerState），则清空该key的所有状态
         * 表示该key对应所有数据的状态已经过期，可以进行清理。
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Post, Comment>> out) throws Exception {
            // 只处理处理时间定时器
            if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME) {
                if (cleanupTimerState.value() != null && timestamp == cleanupTimerState.value()) {
                    postValueState.clear();
                    commentListState.clear();
                    cleanupTimerState.clear();
                }
            }
        }


        // 使用处理时间注册定时器
        private void resetCleanupTimer(Context ctx) throws Exception {
            Long currentTimer = cleanupTimerState.value();
            if (currentTimer != null) {
                ctx.timerService().deleteProcessingTimeTimer(currentTimer);
            }

            // 使用处理时间 + 20秒作为清理时间
            long newCleanupTime = ctx.timerService().currentProcessingTime() + 20000L;
            ctx.timerService().registerProcessingTimeTimer(newCleanupTime);
            cleanupTimerState.update(newCleanupTime);
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
