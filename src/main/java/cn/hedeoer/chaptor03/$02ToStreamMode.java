package cn.hedeoer.chaptor03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class $02ToStreamMode {
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
        Table result = tableEnv.sqlQuery("select data.order_status, sum(origin_amount) as total_amount  from order_info where data.order_status is not null group by data.order_status " );

        //12> -U[1001, 720800.0]
        //12> +U[1001, 721000.0]
        //12> -U[1001, 721000.0]
        //12> +U[1001, 721200.0] -U表示删除（delete）操作后续应该删除，+U表示更新（update）或插入（insert）操作
        tableEnv.toChangelogStream(result).print();

        //5> (false,(1002,358000.0))
        //5> (true,(1002,358200.0))
        //19> (false,(1003,291000.0))
        //19> (true,(1003,291200.0))
        //5> (false,(1002,358200.0)) true 表示add操作，false 表示撤回操作
        //5> (true,(1002,358600.0))
//        tableEnv.toRetractStream(result, Types.TUPLE(Types.STRING, Types.DOUBLE)).print();

        env.execute();

    }
}
