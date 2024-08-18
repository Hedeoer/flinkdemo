package cn.hedeoer.chaptor03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * flink sql中 时间属性关于处理时间， 事件时间的定义
 * ·1. 流转化为表，水印的提取和生成需要在流中定义
 *  2. 在flink sql的DDL中定义
 *      2.1 事件时间 WATERMARK FOR `eventTime` AS `eventTime` - INTERVAL '5' SECOND；使用的字段必须为timestamp，还可以推迟水印的延迟
 *      2.2 处理时间 proctime AS PROCTIME()
 */
public class $03TimeProperties {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 注册临时表1,使用流记录中字段作为事件时间
//TableResult tableResult1 = tableEnv.executeSql("CREATE TEMPORARY TABLE order_info1 (\n" +
//        "    `database` STRING,\n" +
//        "    `table` STRING,\n" +
//        "    `type` STRING,\n" +
//        "    `ts` BIGINT,\n" +
//        "    `xid` BIGINT,\n" +
//        "    `commit` BOOLEAN,\n" +
//        "    `data` ROW<id BIGINT, user_id BIGINT, origin_amount DOUBLE, coupon_reduce DOUBLE, final_amount DOUBLE, order_status STRING, out_trade_no STRING, trade_body STRING, session_id STRING, province_id INT, create_time STRING, expire_time STRING, update_time STRING>,\n" +
//        "    `old` ROW<order_status STRING, update_time STRING>,\n" +
//        "    `eventTime` AS TO_TIMESTAMP_LTZ(`ts` * 1000, 3),  -- 使用 ts 字段生成事件时间\n" +
//        "    WATERMARK FOR `eventTime` AS `eventTime` - INTERVAL '5' SECOND -- 定义 Watermark\n" +
//        ") WITH (\n" +
//        "    'connector' = 'kafka',\n" +
//        "    'topic' = 'topic_db',\n" +
//        "    'properties.bootstrap.servers' = 'hadoop104:9092',\n" +
//        "    'properties.group.id' = 'test_group',\n" +
//        "    'format' = 'json',\n" +
//        "    'scan.startup.mode' = 'earliest-offset'\n" +
//        ")\n");

        // 注册临时表2，处理时间
        TableResult tableResult2 = tableEnv.executeSql("CREATE TEMPORARY TABLE order_info2 (\n" +
                "    `database` STRING,\n" +
                "    `table` STRING,\n" +
                "    `type` STRING,\n" +
                "    `ts` BIGINT,\n" +
                "    `xid` BIGINT,\n" +
                "    `commit` BOOLEAN,\n" +
                "    `data` ROW<id BIGINT, user_id BIGINT, origin_amount DOUBLE, coupon_reduce DOUBLE, final_amount DOUBLE, order_status STRING, out_trade_no STRING, trade_body STRING, session_id STRING, province_id INT, create_time STRING, expire_time STRING, update_time STRING>,\n" +
                "    `old` ROW<order_status STRING, update_time STRING>,\n" +
                "     proctime AS PROCTIME() \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'topic_db',\n" +
                "    'properties.bootstrap.servers' = 'hadoop104:9092',\n" +
                "    'properties.group.id' = 'test_group',\n" +
                "    'format' = 'json',\n" +
                "    'scan.startup.mode' = 'earliest-offset'\n" +
                ")\n");


        // | +I |           1645948748 | 2022-02-27 15:59:08.000 |
        //| +I |           1645948748 | 2022-02-27 15:59:08.000 |
        //| +I |           1645948748 | 2022-02-27 15:59:08.000 |

//        tableEnv.executeSql("select ts, eventTime from order_info1").print();

        //| +I |           1645948748 | 2024-08-18 12:59:31.187 |
        //| +I |           1645948748 | 2024-08-18 12:59:31.187 |
        //| +I |           1645948748 | 2024-08-18 12:59:31.187 |
        tableEnv.executeSql("select ts, proctime from order_info2").print();
        env.execute();



    }
}
