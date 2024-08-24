package cn.hedeoer.chaptor03;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * flink表中的时间属性提取，需要Stream中，或者表中的类型为Long，或者 timestamp，否则无法提取水印；时间一般涉及到时区的概念
 * 常见的时间函数：
 * LOCALTIME
 * LOCALTIMESTAMP
 * CURRENT_DATE
 * CURRENT_TIME
 * CURRENT_TIMESTAMP
 * CURRENT_ROW_TIMESTAMP()
 * NOW()
 * PROCTIME()
 */
public class $00_02TimeZone {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("" +
                "SET sql-client.execution.result-mode=tableau;\n" +
                "CREATE VIEW MyView1 AS SELECT LOCALTIME, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_ROW_TIMESTAMP(), NOW(), PROCTIME();\n" +
                "DESC MyView1;").print();

        env.execute();
    }
}
