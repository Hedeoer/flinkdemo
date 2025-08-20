package cn.hedeoer.chaptor06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * datastream api 中对于 sessionwindowjoin在sql中实现
 * Windowing TVFs中的 Session Windows 在flink 1.13 后的版本开始支持，可以查看 https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/dev/table/sql/queries/window-tvf/#session
 * 具体语法为 SESSION(TABLE data [PARTITION BY(keycols, ...)], DESCRIPTOR(timecol), gap)
 *
 */
public class $03SessionWindowJoinBySQL {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        /*
         * flink 1.13版本无 socket类型的table connector，下面为模拟代码
         * */
        TableResult tableResult = tableEnvironment.executeSql("CREATE TABLE green_source (\n" +
                "    `key` INT,\n" +
                "    `event_time_str` STRING,\n" +
                "    `event_time` AS TO_TIMESTAMP(`event_time_str`, 'yyyy-MM-dd HH:mm:ss'),\n" +
                "    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'socket',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '8888',\n" +
                "    'format' = 'csv',\n" +
                "    'csv.field-delimiter' = ','\n" +
                ")");

        tableEnvironment.executeSql("CREATE TABLE orange_source (\n" +
                "    `key` INT,\n" +
                "    `event_time_str` STRING,\n" +
                "    `event_time` AS TO_TIMESTAMP(`event_time_str`, 'yyyy-MM-dd HH:mm:ss'),\n" +
                "    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'socket',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '9999',\n" +
                "    'format' = 'csv',\n" +
                "    'csv.field-delimiter' = ','\n" +
                ");");


        TableResult result = tableEnvironment.executeSql("SELECT\n" +
                "    CONCAT(\n" +
                "        'JOIN SUCCESS! ',\n" +
                "        g.key,\n" +
                "        g.event_time_str,\n" +
                "        o.key,\n" +
                "        o.event_time_str\n" +
                "    ) AS join_result\n" +
                "FROM\n" +
                "    TABLE(SESSION(TABLE green_source, DESCRIPTOR(event_time), INTERVAL '5' SECONDS)) AS g\n" +
                "JOIN\n" +
                "    TABLE(SESSION(TABLE orange_source, DESCRIPTOR(event_time), INTERVAL '5' SECONDS)) AS o\n" +
                "ON\n" +
                "    g.`key` = o.`key`\n" +
                "AND\n" +
                "    g.window_start = o.window_start AND g.window_end = o.window_end");
        result.print();

        env.execute();


    }
}
