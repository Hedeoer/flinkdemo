package cn.hedeoer.chaptor03;


import cn.hedeoer.common.datatypes.Sensor;
import cn.hedeoer.common.datatypes.WaterSensor;
import cn.hedeoer.taskconfig.StreamConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

// regualar_join inner join
/*
*
* 1. 只能是等值链接
* 2. 需要考虑TTL,否则会状态过大
* 3. 流处理中，每条流中每来一次数据都会触发扫描另一张表的全部数据
* */
public class $10RegularJoin_InnerJoin {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("table.exec.state.ttl", "60 s");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 开启checkpoint
        StreamConfig.setCheckPoint(env);

        DataStreamSource<WaterSensor> sensorWater = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10L),
                new WaterSensor("sensor_1", 2000L, 20L),
                new WaterSensor("sensor_2", 3000L, 30L),
                new WaterSensor("sensor_1", 4000L, 40L),
                new WaterSensor("sensor_1", 5000L, 50L),
                new WaterSensor("sensor_2", 6000L, 60L)
        );

        SingleOutputStreamOperator<Sensor> sensorTypes = env.socketTextStream("hadoop103", 9999)
                .map(line -> new Sensor(line.split(",")[0], line.split(",")[1]));

//        DataStreamSource<Sensor> sensorTypes = env.fromElements(
//                new Sensor("sensor_1", "温度传感器"),
//                new Sensor("sensor_2", "湿度传感器")
//        );

        Table t1 = tableEnv.fromDataStream(sensorWater, $("sensorId"), $("ts"), $("waterLine"));
        Table t2 = tableEnv.fromDataStream(sensorTypes, $("id"), $("type"));
        tableEnv.createTemporaryView("t1", t1);
        tableEnv.createTemporaryView("t2", t2);

        tableEnv.executeSql(
                "SELECT t2.id, t2.type, t1.waterLine, t1.ts " +
                        "FROM t1 " +
                        "JOIN t2 ON t1.sensorId = t2.id"
        ).print();


//        findOrder2Payment(tableEnv);


//        env.execute();


    }

    private static void findOrder2Payment(StreamTableEnvironment tableEnv) {
        TableResult payment = tableEnv.executeSql("CREATE TEMPORARY TABLE payment_info (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `xid` BIGINT,\n" +
                "  `commit` BOOLEAN,\n" +
                "  `data` ROW<\n" +
                "    id BIGINT,\n" +
                "    out_trade_no STRING,\n" +
                "    order_id BIGINT,\n" +
                "    alipay_trade_no STRING,\n" +
                "    total_amount DOUBLE,\n" +
                "    trade_body STRING,\n" +
                "    payment_type STRING,\n" +
                "    payment_status STRING,\n" +
                "    create_time STRING,\n" +
                "    update_time STRING,\n" +
                "    callback_content STRING,\n" +
                "    callback_time STRING\n" +
                "  >\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'hadoop104:9092',\n" +
                "  'properties.group.id' = 'payment_group',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'earliest-offset'\n" +
                ")");


        TableResult order = tableEnv.executeSql("CREATE TEMPORARY TABLE order_info (\n" +
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


        tableEnv.executeSql(
                "SELECT " +
                        "order_info.data.id as order_id, " +
                        "order_info.data.create_time as order_time, " +
                        "payment_info.id AS payment_id, " +
                        "payment_info.order_id AS order_id_from_pay, " +
                        "payment_info.total_amount AS payment_amount " +
                        "FROM order_info AS order_info " +
                        "INNER JOIN payment_info AS payment_info " +
                        "ON order_info.data.id = payment_info.data.order_id" + " limit 50"
        ).print();
    }
}
