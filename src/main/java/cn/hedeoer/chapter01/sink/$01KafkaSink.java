package cn.hedeoer.chapter01.sink;

import cn.hedeoer.chapter01.source.$03UserDefinedSource;
import cn.hedeoer.common.datatypes.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/*
*
* flink 使用 Kafka Producer 写数据到的kafka
* */
public class $01KafkaSink {

    @Test
    public void testKafkaSink() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.addSource(new $03UserDefinedSource.EventGenerator());

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop103:9092");
        properties.setProperty("num.partitions", "2");
        // 保证失败重试后kafka的数据不会丢失（在flink任务中，当任务失败到任务重启的时间大于kafka的事务时间，则会出现数据丢失），需要合理设置参数
        // 强烈建议将 Kafka 的事务超时时间调整至远大于 checkpoint 最大间隔 + 最大重启时间
        properties.setProperty("transaction.timeout.ms", "600000"); // 默认1小时
        properties.setProperty("transaction.max.timeout.ms", "3600000"); // 默认15分钟

        KafkaSerializationSchema<Event> serializationSchema = new KafkaSerializationSchema<Event>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Event event, @Nullable Long aLong) {
                return new ProducerRecord<>(
                        "test_topic",
                        event.user.getBytes(StandardCharsets.UTF_8),
                        event.url.getBytes(StandardCharsets.UTF_8)
                );
            }
        };

        FlinkKafkaProducer<Event> myProducer = new FlinkKafkaProducer<>(
                "test_topic",             // target topic
                serializationSchema,    // serialization schema
                properties,             // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

        source.addSink(myProducer);


        env.execute();
    }
}
