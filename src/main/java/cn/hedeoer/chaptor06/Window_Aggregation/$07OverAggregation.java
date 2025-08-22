package cn.hedeoer.chaptor06.Window_Aggregation;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 含有over子句的聚合
 * 1.over子句中必须有order by子句
 * 2.对于流式查询，必须使用递增的时间属性来定义 OVER 窗口
 * 3.不支持多字段的 order by 排序，比如 order by a, b
 */
public class $07OverAggregation {
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
//                "    'scan.startup.specific-offsets' = 'partition:0,offset:844',\n" +
                "    'value.format' = 'json',\n" +
                "       'value.json.timestamp-format.standard' = 'ISO-8601'" +
                ")");
/*
select
order_time,
total,
sum(total) over (partition by user_id) as sum_total
from purchases
* */
        // 必须拥有order by 字句
//        tableEnv.executeSql("select\n" +
//                "order_time,\n" +
//                "total,\n" +
//                "sum(total) over (partition by user_id) as sum_total\n" +
//                "from purchases").print();


/*
select
user_id,
order_time,
total,
sum(total) over (partition by user_id order by order_time desc) as sum_total
from purchases
* */
        // 必须使用递增的时间属性来定义 OVER 窗口,否则报错：“The window can only be ordered in ASCENDING mode.”
//        tableEnv.executeSql("select\n" +
//                "user_id,\n" +
//                "order_time,\n" +
//                "total,\n" +
//                "sum(total) over (partition by user_id order by order_time desc) as sum_total\n" +
//                "from purchases").print();



/*
select
user_id,
order_time,
total,
row_number() over (order by order_time,user_id) as order_number
from purchases
* */
        // 不支持多字段的 order by 排序，比如 order by a, b 否则报错：The window can only be ordered by a single time column.
//        tableEnv.executeSql("select\n" +
//                "user_id,\n" +
//                "order_time,\n" +
//                "total,\n" +
//                "row_number() over (order by order_time,user_id) as order_number\n" +
//                "from purchases").print();

/*
select
    user_id,
    order_time,
    total,
    row_number() over (partition by user_id order by total asc ) as order_number
from purchases
* */
        // 流模式下必须使用递增的时间属性来定义 OVER 窗口，“OVER windows' ordering in stream mode must be defined on a time attribute.”
//        tableEnv.executeSql("select\n" +
//                "    user_id,\n" +
//                "    order_time,\n" +
//                "    total,\n" +
//                "    row_number() over (partition by user_id order by total asc ) as order_number\n" +
//                "from purchases").print();

/*
select
user_id,
order_time,
total
from (
    select
    user_id,
    order_time,
    total,
    row_number() over (partition by user_id order by total desc ) as order_number
    from purchases) t1
where order_number <= 2
* */

        // 对flink 中  topN的计算需要严格按照特定的查询格式，flink才会识别为topN查询，在topN查询中order by 子句可以指定非时间属性，并且可以指定倒序，并且可以指定多个字段排序
        // https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/topn/
//        tableEnv.executeSql("select\n" +
//                "user_id,\n" +
//                "order_time,\n" +
//                "total\n" +
//                "from (\n" +
//                "    select\n" +
//                "    user_id,\n" +
//                "    order_time,\n" +
//                "    total,\n" +
//                "    row_number() over (partition by user_id order by total desc ) as order_number\n" +
//                "    from purchases) t1\n" +
//                "where order_number <= 2").print();


/*

select
user_id,
order_time,
total
from purchases
where user_id in(1,2)
order by order_time,user_id

* */
        // 在流处理模式下，表的主排序顺序必须按时间属性升序排列，后续排序可自由选择。但批处理模式无此限制,否则报错：Sort on a non-time-attribute field is not supported.
//        tableEnv.executeSql("select\n" +
//                "user_id,\n" +
//                "order_time,\n" +
//                "total\n" +
//                "from purchases\n" +
//                "where user_id in(1,2)\n" +
//                "order by user_id,order_time" ).print();
    }
}
