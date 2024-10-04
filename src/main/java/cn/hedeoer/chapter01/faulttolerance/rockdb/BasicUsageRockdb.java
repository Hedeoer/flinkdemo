package cn.hedeoer.chapter01.faulttolerance.rockdb;

import cn.hedeoer.common.datatypes.VideoPlayProgress;
import cn.hedeoer.common.sources.VideoPlayProgressSource;
import cn.hedeoer.common.utils.TimeFormat;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 注意引入相关依赖：
 * <dependency>
 * <groupId>org.apache.hadoop</groupId>
 * <artifactId>hadoop-client</artifactId>
 * <version>${hadoop-version}</version>
 * <scope>provided</scope>
 * </dependency>
 *
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-statebackend-rocksdb_${scala.binary.version}</artifactId>
 * <version>${flink.version}</version>
 * </dependency>
 * 1. rocksdb启用方式：
 * 1.1 可以单独为每个flink任务启用rocksdb
 * 1.2 也可以为所有的flink集群任务通过配置集群的flink配置文件调整rocksdb
 * <p>
 * 2. 如果启用了rocksdb，但没有设置外部的文件存储路径，则默认使用flink的托管内存的大小，但一般使用rocksdb都要配置外面文件系统作为状态存储方法
 * 3. rockdb存储数据时，默认的key，value都是字节数组，java中字节数组的元素个数上限理论上为Integer.MAX_VALUE(2<sup>31</sup>-1)因此在存储一些大状态时要注意长度
 * <p>
 * <p>
 * rocksdb作为状态后端的优化：
 * 1. 启用增量快照
 * 2. 默认使用rocksdb，如果启用了外部存储系统，定时器存储在外部的文件系统，不要频繁的访问定时器的信息，可能会造成程序的瓶颈，可以配置将定时器的存储转移到jvm堆上
 * 3. flink提供了精细化配置rocksdb的方法，比如控制RocksDB的ColumnFamily ，MemTable 等方法，可以查看ConfigurableRocksDBOptionsFactory
 */
public class BasicUsageRockdb {
    /**
     * 每5秒统计一次过去处理的数据条数
     * @param args
     * @throws Exception
     */
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
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck/rockdbs");


        env.addSource(new VideoPlayProgressSource())
                .map(new MyMapMetric())
                .keyBy(value -> "global")
                .process(new KeyedProcessFunction<String, VideoPlayProgress, Tuple3<String, String, String>>() {

                    private ValueState<Long> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("countState", Long.class, 0L));
                    }

                    @Override
                    public void processElement(VideoPlayProgress value, KeyedProcessFunction<String, VideoPlayProgress, Tuple3<String, String, String>>.Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {

                        // 定义一个基于物理时间的5秒定时器
                        ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000L);
                        countState.update(countState.value() + 1L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, VideoPlayProgress, Tuple3<String, String, String>>.OnTimerContext ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                        // 开始时间，触发时间，数据条数
                        out.collect(Tuple3.of(TimeFormat.longToString(timestamp - 5000L), TimeFormat.longToString(timestamp ),  "" + countState.value()));
                        countState.clear();
                        // 删除定时器,定时器到期可以自动删除，此处只是手动删除
                        ctx.timerService().deleteProcessingTimeTimer(timestamp);



                    }
                })
                .print();


        env.execute();
    }

    /**
     * 启动metric监控
     */
    public static class MyMapMetric extends RichMapFunction<VideoPlayProgress, VideoPlayProgress> {

        private transient Counter counter;

        @Override
        public void open(Configuration parameters) throws Exception {

            this.counter = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("state.backend.rocksdb.timer-service.factory")
                    .counter("rocksdbTimerService");


        }

        @Override
        public VideoPlayProgress map(VideoPlayProgress value) throws Exception {

            this.counter.inc();
            return value;
        }
    }
}
