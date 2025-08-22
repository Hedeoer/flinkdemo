package cn.hedeoer.chaptor06.Window_Aggregation;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 可以使用级联不同窗口聚合的结果实现类似roll up的效果
 *
 * 从flink 1.13开始，推荐使用 window tvf实现聚合操作，不在推荐使用 group by 中使用window函数实现聚合操作（https://nightlies.apache.org/flink/flink-docs-release-1.13/zh/docs/dev/table/sql/queries/window-agg/#group-window-aggregation）
 *
 * -- tumbling 5 minutes for each supplier_id
 * CREATE VIEW window1 AS
 * SELECT window_start, window_end, window_time as rowtime, SUM(price) as partial_price
 *   FROM TABLE(
 *     TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '5' MINUTES))
 *   GROUP BY supplier_id, window_start, window_end, window_time;
 *
 * -- tumbling 10 minutes on the first window
 * SELECT window_start, window_end, SUM(partial_price) as total_price
 *   FROM TABLE(
 *       TUMBLE(TABLE window1, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))
 *   GROUP BY window_start, window_end;
 */
public class $06WindowTVFAggregation {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("rest.port", "8081");

        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // 为了方便观察，并行度设为1
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置状态的存活时间
//        tableEnv.getConfig().getConfiguration().setString("table.exec.state.ttl", "5 s");

        // --- 创建 users 表 ---
        tableEnv.executeSql("CREATE TABLE users (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    email STRING,\n" +
                "    city STRING,\n" +
                "    state STRING,\n" +
                "    zipcode STRING" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'mz_datagen_ecommerce_users',\n" +
                "    'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "    'properties.group.id' = 'flink-consumer-group-users',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json'\n" +
                ")");

        // --- 创建 purchases 表 ---
        tableEnv.executeSql("CREATE TABLE purchases (\n" +
                "    id STRING,\n" +
                "    user_id INT,\n" +
                "    item_ids ARRAY<INT>,\n" +
                "    total DECIMAL(10, 2),\n" +
                "    order_time TIMESTAMP_LTZ(3) ,\n" +
                "    WATERMARK FOR order_time AS order_time - INTERVAL '1' HOUR " +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'mz_datagen_ecommerce_purchases',\n" +
                "    'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "    'properties.group.id' = 'flink-consumer-group-purchases',\n" +
                "    'scan.startup.mode' = 'specific-offsets',\n" +
                "    'scan.startup.specific-offsets' = 'partition:0,offset:844',\n" +
                "    'value.format' = 'json',\n" +
                "       'value.json.timestamp-format.standard' = 'ISO-8601'" +
                ")");

        // --- 创建 items 表 ---
        tableEnv.executeSql("CREATE TABLE items (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    price DECIMAL(10, 2),\n" +
                "    description STRING,\n" +
                "    material STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'mz_datagen_ecommerce_items',\n" +
                "    'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "    'properties.group.id' = 'flink-consumer-group-items',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json'\n" +
                ")");
/*

select
    window_start, window_end,sum(partial_price) as total_price
from table(
        tumble( table window1, descriptor(rowtime),interval '1' day ))
group by window_start, window_end

* */
        // 执行查询并打印结果，这将触发 Flink 作业的执行
        tableEnv.executeSql("CREATE VIEW window1 AS " +
                "SELECT " +
                "  window_start AS hourly_window_start, " + // 重命名
                "  window_end AS hourly_window_end, " +     // 重命名
                "  window_time AS rowtime, " +
                "  SUM(total) AS partial_price " +
                "FROM TABLE(TUMBLE(TABLE purchases, DESCRIPTOR(order_time), INTERVAL '1' HOUR)) " +
                "GROUP BY window_start, window_end, window_time");

        tableEnv.executeSql("SELECT " +
                "  window_start, " +
                "  window_end, " +
                "  SUM(partial_price) AS total_price " +
                "FROM TABLE( " +
                "    TUMBLE( TABLE window1, DESCRIPTOR(rowtime), INTERVAL '1' DAY )) " +
                "GROUP BY window_start, window_end").print();


    }
}