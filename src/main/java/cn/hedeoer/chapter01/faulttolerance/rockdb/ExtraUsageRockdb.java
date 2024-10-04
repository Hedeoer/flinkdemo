package cn.hedeoer.chapter01.faulttolerance.rockdb;

import cn.hedeoer.common.datatypes.VideoPlayProgress;
import cn.hedeoer.common.sources.VideoPlayProgressSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicLong;

/*
模拟rockdb的key，value存储时，如果大小超过Integer.maxValue时，的问题。
 */
public class ExtraUsageRockdb {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        Configuration configuration = new Configuration();
        ConfigOption<String> rocksdb = ConfigOptions
                .key("state.backend.rocksdb.timer-service.factory")
                .stringType()
                .defaultValue("ROCKSDB")
                .withDescription("This determines the factory for timer service state implementation. Options are either HEAP (heap-based) or ROCKSDB for an implementation based on RocksDB.");

        configuration.set(rocksdb, "HEAP");

        configuration.setString("rest.port", "8088");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);


        // 方式1. 开启rockdb状态后端，并开启增量备份
        env.enableCheckpointing(5000);
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(embeddedRocksDBStateBackend);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/rockdbs");


        env.addSource(new VideoPlayProgressSource())
                // 使用map将所有的videoid设置为1
                .map(new MapFunction<VideoPlayProgress, VideoPlayProgress>() {
                    @Override
                    public VideoPlayProgress map(VideoPlayProgress value) throws Exception {
                        value.setVideoId(1);
                        return value;
                    }
                })
                .keyBy(video -> video.getVideoId())
                .process(new VideoProgressProcessFunction()).print();

        env.execute();
    }

    // 自定义的 ProcessFunction，累积大量数据
    public static class VideoProgressProcessFunction extends ProcessFunction<VideoPlayProgress, Long> {

        private transient ListState<String> listState;
        private static final long MAX_STATE_SIZE = (long) Integer.MAX_VALUE; // 2^31 字节
        private AtomicLong currentStateSize = new AtomicLong(0);

        @Override
        public void open(Configuration parameters) throws Exception {
            // 定义 ListStateDescriptor
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                    "videosList",
                    TypeInformation.of(new TypeHint<String>() {
                    })
            );
            listState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(VideoPlayProgress value, Context ctx, Collector<Long> out) throws Exception {
            // 添加大量小字符串
            for (int i = 0; i < 1100000000; i++) { // 调整循环次数以加快达到限制
                String pos = value.getPositionSec() + "_" + i;
                listState.add(pos);
                currentStateSize.addAndGet(pos.getBytes("UTF-8").length);
                if (currentStateSize.get() >= MAX_STATE_SIZE) {
                    // 达到或超过 2^31 字节限制，抛出异常或采取其他措施
                    throw new IllegalStateException("State size exceeds the maximum limit of 2^31 bytes.");
                }
            }
            out.collect(currentStateSize.get());
        }
    }
}
