package cn.hedeoer.chaptor03;


import cn.hedeoer.common.datatypes.Sensor;
import cn.hedeoer.common.datatypes.WaterSensor;
import cn.hedeoer.taskconfig.StreamConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

// regualar_join left join

public class $11RegularJoin_LeftJoin {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("table.exec.state.ttl", "5 s");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
                        "left JOIN t2 ON t1.sensorId = t2.id"
        ).print();


    }

}
