package cn.hedeoer.chaptor06.Window_Aggregation;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * flink 除了支持topn的计算：https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/topn/
 * 还支持特定窗口内的topn计算:https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/window-topn/
 *
 * flink1.13版本window topn限制：
 * 支持对窗口内进行过聚合计算的列进行topn排序
 *
 * 截止flink 2.1 ：https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/dev/table/sql/queries/window-topn/
 * 仅支持 tumbling window（滚动窗口），top window（滑动窗口），Cumulate Windows（累计窗口）后的topn计算,对于Session windows（会话窗口）后的topN依旧没有实现
 *
 */
public class $08WindowTopN {
    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        configuration.setString("rest.port", "8081");
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // 为了方便观察，并行度设为1
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration config = tableEnv.getConfig().getConfiguration();
        // 配置状态的 TTL
        // table.exec.state.ttl 是基于系统处理时间（Processing Time）来清理过期状态的，而不是基于事件时间（Event Time）
        config.setString("table.exec.state.ttl", "10000");

        tableEnv.executeSql("CREATE TABLE purchases (\n" +
                "    id STRING,\n" +
                "    user_id INT,\n" +
                "    item_ids ARRAY<INT>,\n" +
                "    total DECIMAL(10, 2),\n" +
                "    order_time TIMESTAMP_LTZ(3) ,\n" +
                "    WATERMARK FOR order_time AS order_time - INTERVAL '0' SECOND " +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'mz_datagen_ecommerce_purchases',\n" +
                "    'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "    'properties.group.id' = 'flink-consumer-group-purchases',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',\n" +
                "       'value.json.timestamp-format.standard' = 'ISO-8601'" +
                ")");
/*
select
    user_id,
    window_start,
    window_end,
    spent_money,
    purchase_items_number,
    row_num
from (
    select
    *,
    row_number() over(partition by window_start,window_end order by spent_money desc ) as row_num
    from (
        select
            window_start,
            window_end,
            user_id,
            sum(total) as spent_money,
            sum(CARDINALITY(item_ids)) as purchase_items_number
        from table (tumble(table purchases,descriptor(order_time), interval '10' seconds))
        group by window_start,window_end,user_id
    )
) t1
where row_num <=3
* */

        tableEnv.executeSql("select\n" +
                "    user_id,\n" +
                "    window_start,\n" +
                "    window_end,\n" +
                "    spent_money,\n" +
                "    purchase_items_number,\n" +
                "    row_num\n" +
                "from (\n" +
                "    select\n" +
                "    *,\n" +
                "    row_number() over(partition by window_start,window_end order by spent_money desc ) as row_num\n" +
                "    from (\n" +
                "        select\n" +
                "            window_start,\n" +
                "            window_end,\n" +
                "            user_id,\n" +
                "            sum(total) as spent_money,\n" +
                "            sum(CARDINALITY(item_ids)) as purchase_items_number\n" +
                "        from table (tumble(table purchases,descriptor(order_time), interval '30' seconds))\n" +
                "        group by window_start,window_end,user_id\n" +
                "    )\n" +
                ") t1\n" +
                "where row_num <=3").print();
    }
}
