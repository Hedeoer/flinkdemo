package cn.hedeoer.chapter01.source;

import cn.hedeoer.common.datatypes.OrderInfo;
import cn.hedeoer.common.utils.TimeFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;


// kafka connector，读取kafka数据
/*
 *
 * 默认kafkasource的模式是在无界流场景，只要flink job不失败或者取消，kafkasource就会一直工作
 *， 也可以setBounded设置读取的offset位置，形成有界流，实现批处理的效果
 *
 * 1. kafkasource并行度的设置
 *    通常根据读取的kafka主题的分区保持一致，如果kafkasource算子的并行度 > kafka的分区数，如果使用kafka记录中的字段作为水印标准，可能会造成水印的无法更新
 *    在这种情况下，可以考虑算子的赋闲（idleness）
 *
 * 2. kafkasource对消费offset的提交
 *    kafkasource会自动提交offset，但是如果flink job开启了checkpoint，那么提交offset的时机是，每次checkpoint之后；如果没有设置checkpoint，则kafkasource内部会根据配置的相应参数周期性提交消费进度（offset）
 *    参数：enable.auto.commit， auto.commit.interval.ms
 * 3.如果在使用kafkasource，kafkaproduce过程中出现kafka的数据丢失，着重查看以下kafka参数：
 *   acks
 *   log.flush.interval.messages
 *   log.flush.interval.ms
 *   log.flush.*
 *
 * */

public class $01KafkaSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 提取记录中的某个字段作为事件时间戳并分配水印
        WatermarkStrategy<String> strategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {

                    private long ts;

                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        ObjectMapper mapper = new ObjectMapper();
                        try {
                            JsonNode jsonNode = mapper.readTree(element);
                            ts = jsonNode.get("ts").asLong() * 1000;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return ts;
                    }
                })
                // 此处flink测试环境的并行度为24，kafka主题topic_db有3个分区，如果不设置赋闲时间，则不会触发后面的窗口计算
                .withIdleness(Duration.ofSeconds(5));


        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop103:9092")
                // 主题或者分区的订阅，可以同时订阅多个主题，也可以只订阅一个主题的多个分区
                .setTopics("topic_db")
                // 动态的分区发现，可以在主题下的分区增加时，自动发现
                .setProperty("partition.discovery.interval.ms", "10000")
                // 指定一个消费者组
                .setGroupId("log_group")
                // 消费的方式
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 反序列化的方式
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        //    private Long id;                // 编号（主键）
        //    private Long userId;            // 用户id
        //    private BigDecimal originAmount; // 原始金额
        //    private BigDecimal couponReduce; // 优惠券减免
        //    private BigDecimal finalAmount;  // 最终金额
        //    private String orderStatus;      // 订单状态
        //    private String outTradeNo;       // 订单交易编号（第三方支付用）
        //    private String tradeBody;        // 订单描述（第三方支付用）
        //    private String sessionId;        // 会话id
        //    private Integer provinceId;      // 省份id
        //    private String createTime; // 创建时间
        //    private String expireTime; // 失效时间
        //    private String updateTime; // 更新时间

        // 每隔1天计算每个省份的订单总金额
        env.fromSource(source, strategy, "Kafka Source")
                // string ==> JsonNode
                .flatMap(new FlatMapFunction<String, OrderInfo>() {
                    @Override
                    public void flatMap(String value, Collector<OrderInfo> out) throws Exception {
                        JsonNode node = new ObjectMapper().readTree(value);
                        String database = node.get("database").asText();
                        String table = node.get("table").asText();
                        parseStringToPojo(out, database, table, node);
                    }
                })
                .map(orderinfo -> Tuple2.<Integer, BigDecimal>of(orderinfo.getProvinceId(), orderinfo.getFinalAmount()))
                .returns(Types.TUPLE(Types.INT, Types.BIG_DEC))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
//                .aggregate(new ProvinceAggregation(), new MyWindowFunction())
                .reduce(new ProvinceReduction(),new MyWindowFunction())
                .print();
        // 24
//        System.out.println(env.getParallelism());
        env.execute();

    }

    private static class MyWindowFunction extends ProcessWindowFunction<Tuple2<Integer, BigDecimal>, Tuple4<String, String, Integer, BigDecimal>, Integer, TimeWindow> {
        @Override
        public void process(Integer provinceId,
                            ProcessWindowFunction<Tuple2<Integer, BigDecimal>, Tuple4<String, String, Integer, BigDecimal>, Integer, TimeWindow>.Context context,
                            Iterable<Tuple2<Integer, BigDecimal>> elements,
                            Collector<Tuple4<String, String, Integer, BigDecimal>> out) throws Exception {

            String start = TimeFormat.longToString(context.window().getStart());
            String end = TimeFormat.longToString(context.window().getEnd());

            for (Tuple2<Integer, BigDecimal> element : elements) {
                out.collect(Tuple4.of(start, end, provinceId, element.f1));
            }
        }
    }

    private static class ProvinceReduction implements ReduceFunction<Tuple2<Integer, BigDecimal>>{
        @Override
        public Tuple2<Integer, BigDecimal> reduce(Tuple2<Integer, BigDecimal> value1, Tuple2<Integer, BigDecimal> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1.add(value2.f1));
        }
    }

    private static class ProvinceAggregation implements AggregateFunction<Tuple2<Integer, BigDecimal>, Tuple2<Integer, BigDecimal>, Tuple2<Integer, BigDecimal>> {

        @Override
        public Tuple2<Integer, BigDecimal> createAccumulator() {
            return new Tuple2(0, BigDecimal.ZERO);
        }

        @Override
        public Tuple2<Integer, BigDecimal> add(Tuple2<Integer, BigDecimal> value, Tuple2<Integer, BigDecimal> accumulator) {
            accumulator.f1 = accumulator.f1.add(value.f1);
            return accumulator;
        }

        @Override
        public Tuple2<Integer, BigDecimal> getResult(Tuple2<Integer, BigDecimal> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<Integer, BigDecimal> merge(Tuple2<Integer, BigDecimal> a, Tuple2<Integer, BigDecimal> b) {
            b.f1 = a.f1.add(b.f1);
            return b;
        }
    }

    public static void parseStringToPojo(Collector<OrderInfo> out, String database, String table, JsonNode node) {
        if ("edu".equals(database) && "order_info".equals(table)) {
            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setId(node.get("data").get("id").asLong());
            orderInfo.setUserId(node.get("data").get("user_id").asLong());
            orderInfo.setOriginAmount(BigDecimal.valueOf(node.get("data").get("origin_amount").asDouble()));
            orderInfo.setCouponReduce(BigDecimal.valueOf(node.get("data").get("coupon_reduce").asDouble()));
            orderInfo.setFinalAmount(BigDecimal.valueOf(node.get("data").get("final_amount").asDouble()));
            orderInfo.setOrderStatus(node.get("data").get("order_status").asText());
            orderInfo.setOutTradeNo(node.get("data").get("out_trade_no").asText());
            orderInfo.setTradeBody(node.get("data").get("trade_body").asText());
            orderInfo.setSessionId(node.get("data").get("session_id").asText());
            orderInfo.setProvinceId(node.get("data").get("province_id").asInt());
            String createTimeStr = node.get("data").get("create_time").asText();
            String updateTimeStr = node.get("data").get("update_time").asText();
            String expireTimeStr = node.get("data").get("expire_time").asText();

            orderInfo.setCreateTime(createTimeStr);
            orderInfo.setUpdateTime(updateTimeStr);
            orderInfo.setExpireTime(expireTimeStr);

            out.collect(orderInfo);
        }
    }
}
