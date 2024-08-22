package cn.hedeoer.chaptor03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;

/**
 * Temporal_Join使用
 * 1. 区别于interval join， temporal join适用于任何形式的动态表
 * 2. temporal join的中状态的生命周期取决于状态后端的设置和表TTL的设置。通常都需要设置表的TTL
 * 3. 一般要求，temporal join的版本表需要主键，相当于数据仓库中的维度表，每个维度都需要“主键“
 * 4. 数据join选取规则（选择最新的版本记录）
 *  4.1 如果使用事件时间如果在某时刻版本表中有多条记录与之匹配，则选择与基表事件时间最匹配的一条版本记录
 *  4.2 如果使用处理时间，则选择与基表处理时间最匹配的一条版本记录
 *
 * 5. 数据的删除
 *    随着时间推移，版本表中过期的记录将被清理
 */
public class $13TemporalJoinExample {

    public static void main(String[] args) throws Exception {
        // 设置 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建订单数据流
        DataStream<Order> orderStream = env.addSource(new OrderSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        // 创建汇率数据流
        DataStream<ExchangeRate> rateStream = env.addSource(new ExchangeRateSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        // 将订单数据流转换为表，并添加水印
        Table orderTable = tableEnv.fromDataStream(orderStream, Schema.newBuilder()
                .column("orderId", DataTypes.STRING())
                .column("orderTime", DataTypes.TIMESTAMP_LTZ(3))
                .column("amount", DataTypes.FLOAT())
                .column("currency", DataTypes.STRING())
                .watermark("orderTime", "orderTime")
                .build());

        // 将汇率数据流转换为表，并添加水印
        Table rateTable = tableEnv.fromDataStream(rateStream, Schema.newBuilder()
                .column("currency", DataTypes.STRING().notNull())
                .primaryKey("currency")
                .column("rate", DataTypes.FLOAT())
                .column("rateTime", DataTypes.TIMESTAMP_LTZ(3))
                .watermark("rateTime", "rateTime")
                .build());

        // 注册为临时视图
        tableEnv.createTemporaryView("Orders", orderTable);
        tableEnv.createTemporaryView("ExchangeRates", rateTable);

        // 执行 Temporal Join 查询
        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        "o.orderId, " +
                        "o.orderTime, " +
                        "o.amount, " +
                        "o.currency, " +
                        "r.rateTime, " +
                        "o.amount * r.rate AS convertedAmount " +
                        "FROM Orders AS o " +
                        "LEFT JOIN ExchangeRates FOR SYSTEM_TIME AS OF o.orderTime AS r " +
                        "ON o.currency = r.currency");

        // 将结果表转换为数据流并打印
        tableEnv.toDataStream(result).print();

        // 执行程序
        env.execute("Flink Temporal Join Example");
    }

    // 模拟订单数据源
    public static class OrderSource implements SourceFunction<Order> {
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            ctx.collect(new Order("1", Instant.parse("2024-01-01T12:00:00Z"), 100.0f, "EUR"));
            ctx.collect(new Order("2", Instant.parse("2024-01-01T12:01:00Z"), 200.0f, "JPY"));
            ctx.collect(new Order("3", Instant.parse("2024-01-01T12:04:00Z"), 200.0f, "JPY"));
            ctx.collect(new Order("3", Instant.parse("2024-01-01T12:00:00Z"), 200.0f, "JPY"));
            Thread.sleep(1000);
        }

        @Override
        public void cancel() {
        }
    }

    // 模拟汇率数据源
    public static class ExchangeRateSource implements SourceFunction<ExchangeRate> {
        @Override
        public void run(SourceContext<ExchangeRate> ctx) throws Exception {
            ctx.collect(new ExchangeRate("EUR", 1.1f, Instant.parse("2024-01-01T12:00:00Z")));
            ctx.collect(new ExchangeRate("JPY", 2.0f, Instant.parse("2024-01-01T12:00:00Z")));
            ctx.collect(new ExchangeRate("JPY", 0.009f, Instant.parse("2024-01-01T12:01:00Z")));
            ctx.collect(new ExchangeRate("JPY", 1.0f, Instant.parse("2024-01-01T12:01:00Z")));
            ctx.collect(new ExchangeRate("EUR", 1.1f, Instant.parse("2024-01-01T12:00:00Z")));

            Thread.sleep(1000);
        }

        @Override
        public void cancel() {
        }
    }

    // 订单类
    public static class Order {
        public String orderId;
        public Instant orderTime;
        public float amount;
        public String currency;

        public Order() {
        }

        public Order(String orderId, Instant orderTime, float amount, String currency) {
            this.orderId = orderId;
            this.orderTime = orderTime;
            this.amount = amount;
            this.currency = currency;
        }
    }

    // 汇率类
    public static class ExchangeRate {
        public String currency;
        public float rate;
        public Instant rateTime;

        public ExchangeRate() {
        }

        public ExchangeRate(String currency, float rate, Instant rateTime) {
            this.currency = currency;
            this.rate = rate;
            this.rateTime = rateTime;
        }
    }
}
