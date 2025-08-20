package cn.hedeoer.chaptor03;
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

// interval join
/*
 *
 * 0. 目前interval join只支持基于事件时间的join
 * 1. 连接的表都需要有时间属性字段
 * 2. 只能应用于append only的动态表
 * 3. interval join会自动清理过期的数据，不需要设置表的TTL，按照join算子的watermark来处理，如果join的两个流中若有一个出现事件时间晚于此时join算子的数据，则由于
 *    晚于watermark的状态的数据都被自动清理，无法join，故该条数据没有任何输出，等同于丢弃。
 * 4. 对于非append only的流，可以使用temporay join实现
 * 5. interval join的两个表地位等同，没有主表之分，任何一条流中有元素到来都会触发 interval join
 * */
public class $12Interval_Join {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("web.port", "8081");

        // 注意：新版本的 Flink 不需要手动设置 web.port，可以直接访问 Flink UI
        // 如果需要，可以通过 flink-conf.yaml 配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 为了便于本地测试，并且由于 WaterSensor 的数据是固定的，
        // 这里不再为 sensorWaterStream 分配 Watermark，因为 fromElements 产生的流是有限的，
        // Watermark 不会推进。我们将完全依赖 Table API 的时间属性定义。
        // 创建 WaterSensor 数据流
        SingleOutputStreamOperator<WaterSensor> sensorWaterStream = env.fromElements(
                new WaterSensor("sensor_1", 1723255200000L, 10L), // 2024-08-10 10:00:00
                new WaterSensor("sensor_1", 1723255320000L, 20L), // 2024-08-10 10:05:00
                new WaterSensor("sensor_2", 1723255800000L, 30L), // 2024-08-10 10:10:00
                new WaterSensor("sensor_1", 1723256100000L, 40L), // 2024-08-10 10:15:00
                new WaterSensor("sensor_1", 1723256700000L, 50L), // 2024-08-10 10:25:00
                new WaterSensor("sensor_2", 1723258800000L, 60L)  // 2024-08-10 11:00:00
        );

        // 创建 SensorBling 数据流并转换为 Tuple2<String, Long>
        SingleOutputStreamOperator<Tuple2<String, Long>> sensorBlingStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] split = s.split(",");
                        String sensorId = split[0];
                        Long knockTime = Long.parseLong(split[1].trim()); // 使用 trim() 避免空白字符
                        return new Tuple2<>(sensorId, knockTime);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1));

        // 使用 Schema 定义 SensorBling 表
        // 定义 f1 是一个长整型，然后通过计算列将其转换为 TIMESTAMP_LTZ 并作为 watermark
        Schema sensorBlingSchema = Schema.newBuilder()
                .column("f0", "STRING")
                .column("f1", "BIGINT") // 将 f1 声明为它本来的类型 BIGINT
                .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f1, 3)") // 创建一个计算列 rowtime
                .watermark("rowtime", "rowtime - INTERVAL '0' SECOND") // 在计算列上定义 watermark
                .build();

        Table sensorBlingTable = tableEnv.fromDataStream(sensorBlingStream, sensorBlingSchema);
        tableEnv.createTemporaryView("sensor_bling", sensorBlingTable);

        // 使用 Schema 定义 WaterSensor 表
        // 同样地，将 ts 声明为 BIGINT，然后通过计算列生成事件时间戳
        Schema waterSensorSchema = Schema.newBuilder()
                .column("sensorId", "STRING")
                .column("waterLine", "BIGINT") // 在WaterSensor中是ts，这里应与class字段对应
                .column("ts", "BIGINT") // 将 ts 声明为它本来的类型 BIGINT
                .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(ts, 3)") // 创建一个计算列 rowtime
                .watermark("rowtime", "rowtime - INTERVAL '0' SECOND") // 在计算列上定义 watermark
                .build();

        // fromDataStream 会自动将 POJO 的字段名映射到 Schema 的列名
        // WaterSensor 的字段是 id, ts, vc，需要和 Schema 对应
        // 为了和你原始代码中的 sensorId, waterLine, ts 对应，我将修改 POJO 字段名或 Schema
        // 这里我假设 WaterSensor 的字段名是 id, ts, vc。SQL 中也需要同步修改。
        // 如果 WaterSensor 的字段是 sensorId, ts, waterLine, 请将 Schema 改为 .column("sensorId", "STRING"), .column("ts", "BIGINT"), .column("waterLine", "BIGINT")
        Table t1 = tableEnv.fromDataStream(sensorWaterStream, waterSensorSchema);
        tableEnv.createTemporaryView("sensor_water", t1);

        // 执行 SQL 查询并打印结果
        // SQL 查询中的时间字段需要使用新定义的 rowtime
        String sql = "SELECT t1.sensorId, " + // 假设字段名为 id
                "t1.rowtime, " +
                "t1.waterLine " + // 假设字段名为 vc
                "FROM sensor_water as t1, sensor_bling as t2 " +
                "WHERE t1.sensorId = t2.f0 " +
                "AND t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime";

        /*
        输入：
        sensor_1,1723255205000 成功匹配 sensor_1 的第一条记录
        sensor_2,1723255206000 失败匹配 (ID不匹配)
        sensor_1,1723255211000 失败匹配 (时间太晚，超过10秒窗口)
        sensor_2,1723255808000 成功匹配 sensor_2 的记录
        sensor_1,1723256100000 成功匹配 sensor_1 的第三条记录 (10:15:00)
        sensor_1,1723256110000 成功匹配 sensor_1 的第三条记录 (在窗口边界)
        输出：
        +----+--------------------------------+-------------------------+----------------------+
        | op |                       sensorId |                 rowtime |            waterLine |
        +----+--------------------------------+-------------------------+----------------------+
        | +I |                       sensor_1 | 2024-08-10 10:00:00.000 |                   10 |
        | +I |                       sensor_2 | 2024-08-10 10:10:00.000 |                   30 |
        | +I |                       sensor_1 | 2024-08-10 10:15:00.000 |                   40 |
        | +I |                       sensor_1 | 2024-08-10 10:15:00.000 |                   40 |
        * */
        tableEnv.executeSql(sql).print();
    }
}
