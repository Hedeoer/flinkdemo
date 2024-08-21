package cn.hedeoer.chaptor03;


// interval join
/*
 *
 * 1. 连接的表都需要有时间属性字段
 * 2. 只能应用于append only的动态表
 * 3. interval join会自动清理过期的数据，不需要设置表的TTL
 * 4. 对于非append only的流，可以使用temporay join实现
 * */


import cn.hedeoer.common.datatypes.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


public class $12Interval_Join {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("web.port", "8081");
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 定义水位线策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs());

        // 创建 WaterSensor 数据流
        SingleOutputStreamOperator<WaterSensor> sensorWaterStream = env.fromElements(
                new WaterSensor("sensor_1", 1723255200000L, 10L), // 2024-08-10 10:00:00
                new WaterSensor("sensor_1", 1723255320000L, 20L), // 2024-08-10 10:05:00
                new WaterSensor("sensor_2", 1723255800000L, 30L), // 2024-08-10 10:10:00
                new WaterSensor("sensor_1", 1723256100000L, 40L), // 2024-08-10 10:15:00
                new WaterSensor("sensor_1", 1723256700000L, 50L), // 2024-08-10 10:25:00
                new WaterSensor("sensor_2", 1723258800000L, 60L)  // 2024-08-10 11:00:00
        );/*.assignTimestampsAndWatermarks(watermarkStrategy);*/

        // 创建 SensorBling 数据流并转换为 Tuple2<String, Long>
        SingleOutputStreamOperator<Tuple2<String, Long>> SensorBlingStream = env.socketTextStream("hadoop103", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] split = s.split(",");
                        String sensorId = split[0];
                        Long knockTime = Long.parseLong(split[1]);
                        return new Tuple2<>(sensorId, knockTime);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1));

        Schema sensorBlingSchema = Schema.newBuilder()
                .column("f0", "STRING")
                .column("f1", "TIMESTAMP_LTZ(3)")
                .watermark("f1", "f1 - INTERVAL '0' SECOND")
                .build();

        Table sensorBlingTable = tableEnv.fromDataStream(SensorBlingStream, sensorBlingSchema);
        tableEnv.createTemporaryView("sensor_bling", sensorBlingTable);

//        tableEnv.executeSql("DESCRIBE sensor_bling").print();


        // 使用 Schema 定义 WaterSensor 表
        Schema waterSensorSchema = Schema.newBuilder()
                .column("sensorId", "STRING")
                .column("waterLine", "BIGINT")
                .column("ts", "TIMESTAMP_LTZ(3)")
                .watermark("ts", "ts - INTERVAL '0' SECOND")
                .build();

        // 注意这里需要使用 `fromDataStream` 方法并提供一个类映射
        Table t1 = tableEnv.fromDataStream(sensorWaterStream, waterSensorSchema);
        // 创建临时视图
        tableEnv.createTemporaryView("sensor_water", t1);
//        tableEnv.executeSql("describe sensor_water").print();


        // 执行 SQL 查询并打印结果
        // todo ：Caused by: org.codehaus.commons.compiler.CompileException: Line 12, Column 97: Cannot cast "java.lang.Long" to "java.time.Instant" 测试未通过，浪费时间！！！
        String sql = "SELECT t1.sensorId, " +
                "t1.ts, " +
                "t1.waterLine " +
//                "t2.f1 AS knockTime " +
                "FROM sensor_water as t1, sensor_bling as t2 " +
                "WHERE t1.sensorId = t2.f0 " +
                "AND t1.ts BETWEEN t2.f1 - INTERVAL '10' SECOND AND t2.f1";

        tableEnv.executeSql(sql).print();

    }
}
