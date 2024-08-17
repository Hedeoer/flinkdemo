package cn.hedeoer.chaptor03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class $01BaseUseage {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 注册临时表1
        TableResult tableResult = tableEnv.executeSql("CREATE TEMPORARY TABLE order_info (\n" +
                "    `database` STRING,\n" +
                "    `table` STRING,\n" +
                "    `type` STRING,\n" +
                "    `ts` BIGINT,\n" +
                "    `xid` BIGINT,\n" +
                "    `commit` BOOLEAN,\n" +
                "    `data` ROW<id BIGINT, user_id BIGINT, origin_amount DOUBLE, coupon_reduce DOUBLE, final_amount DOUBLE, order_status STRING, out_trade_no STRING, trade_body STRING, session_id STRING, province_id INT, create_time STRING, expire_time STRING, update_time STRING>,\n" +
                "    `old` ROW<order_status STRING, update_time STRING>\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'topic_db',\n" +
                "    'properties.bootstrap.servers' = 'hadoop104:9092',\n" +
                "    'properties.group.id' = 'test_group',\n" +
                "    'format' = 'json',\n" +
                "    'scan.startup.mode' = 'earliest-offset'\n" +
                ")\n");

        // 注册临时表2
        tableEnv.executeSql("create temporary table order_query(`database` string, `table` string, user_id bigint, origin_amount double) with( 'connector' = 'print' )" );


        // 执行查询sql
        Table result1 = tableEnv.sqlQuery(
                "SELECT `database`, `table`, `data`.`user_id`, `data`.`origin_amount` " +
                        "FROM order_info where `table` = 'order_info'"
        );


        //查询结果加载
        result1.executeInsert("order_query");

        tableEnv.executeSql("select * from  order_query").print();



    }
}
