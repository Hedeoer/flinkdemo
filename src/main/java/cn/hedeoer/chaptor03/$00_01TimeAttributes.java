package cn.hedeoer.chaptor03;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.expressions.PlannerExpressionParserImpl.proctime;

public class $00_01TimeAttributes {

    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tableEnv;
    private static DataStream<User> steam;
    private static DataStream<Student> studentDataStream;

    @BeforeAll
    public static void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnv = StreamTableEnvironment.create(env);
        // create a DataStream
        steam =
                env.fromElements(
                                new User("Alice", 4, Instant.ofEpochMilli(1000)),
                                new User("Bob", 6, Instant.ofEpochMilli(1001)),
                                new User("Alice", 10, Instant.ofEpochMilli(1002)))
                        // 指定水印生成和提取策略
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((element, timestamp) -> element.event_time.toEpochMilli()));

        studentDataStream =
                env.fromElements(
                        new Student("Alice", 4, Instant.ofEpochMilli(1000)),
                        new Student("Bob", 6, Instant.ofEpochMilli(1001)),
                        new Student("Alice", 10, Instant.ofEpochMilli(1002)));
    }

    /**
     * 从流中获取事件时间
     * 1.流中有水印,比如流steam
     * 2.流中无水印，比如流studentDataStream
     */
    @Test
    public void getEventTimeFromStream() {
        //1. 流中有水印,比如流steam，
        // ①可以通过Schema.newBuilder()从元数据获取水印，即SOURCE_WATERMARK()方法；
        // ②也可以直接指定逻辑字段作为时间属性,从元数据获取水印，调用rowtime()方法，逻辑字段可以任意起名，以下命名为row_time
        Table table1 = tableEnv.fromDataStream(steam, Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("score", DataTypes.INT())
                .columnByMetadata("row_time", DataTypes.TIMESTAMP_LTZ(3))
                .watermark("row_time", "SOURCE_WATERMARK()")
                .build());

        // 逻辑字段 row_time
//        Table table1 = tableEnv.fromDataStream(steam, $("name"), $("score"), $("row_time").rowtime());
        tableEnv.createTemporaryView("user_data", table1);

        tableEnv.executeSql("select " +
                "window_start, " +
                "window_end, " +
                "window_time, " +
                "count(1) as  total_nums, " +
                "avg(CAST(score AS DOUBLE)) avg_score , " +
                "sum(score) sum_score " +
                "from table(tumble(table user_data , descriptor(row_time), interval '4' second )) " +
                "group by window_start, window_end, window_time").print();

        //+----+-------------------------+-------------------------+-------------------------+----------------------+--------------------------------+-------------+
        //| op |            window_start |              window_end |             window_time |           total_nums |                      avg_score |   sum_score |
        //+----+-------------------------+-------------------------+-------------------------+----------------------+--------------------------------+-------------+
        //| +I | 1970-01-01 08:00:00.000 | 1970-01-01 08:00:04.000 | 1970-01-01 08:00:03.999 |                    3 |              6.666666666666667 |          20 |
        //+----+-------------------------+-------------------------+-------------------------+----------------------+--------------------------------+-------------+

//        tableEnv.executeSql("select * from user_data").print();

        // 2. 流中无水印，比如流studentDataStream;
        // ①可以指定通过table api的逻辑计算列方法指定时间属性字段，之后分配水印的策略

        Table table2 = tableEnv.fromDataStream(studentDataStream, Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("score", DataTypes.INT())
                .columnByExpression("row_time", "CAST(event_time AS TIMESTAMP_LTZ(3))")
                .watermark("row_time", "row_time - INTERVAL '0' SECOND")
                .build());
        tableEnv.createTemporaryView("student_data", table2);
        tableEnv.executeSql("select " +
                "window_start, " +
                "window_end, " +
                "window_time, " +
                "count(1) as  total_nums, " +
                "avg(CAST(score AS DOUBLE)) avg_score , " +
                "sum(score) sum_score " +
                "from table(tumble(table student_data , descriptor(row_time), interval '4' second )) " +
                "group by window_start, window_end, window_time").print();

    }

    @Test
    public void getProcessTimeFromStream() {

        // 定义 Schema 并将 Processing Time 作为时间字段
        Table table0 = tableEnv.fromDataStream(
                studentDataStream,
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("score", DataTypes.INT())
                        .columnByExpression("process_time", (Expression) proctime())
                        .build()
        );


        Table table1 = tableEnv.fromDataStream(studentDataStream, $("name"), $("score"), $("event_time"), $("process_time").proctime());
        tableEnv.createTemporaryView("student_data", table1);
//        tableEnv.executeSql("select * from student_data").print();
        tableEnv.executeSql("select " +
                "window_start, " +
                "window_end, " +
                "window_time, " +
                "count(1) as  total_nums, " +
                "avg(CAST(score AS DOUBLE)) avg_score , " +
                "sum(score) sum_score " +
                "from table(tumble(table student_data , descriptor(process_time), interval '1' second )) " +
                "group by window_start, window_end, window_time").print();

    }


    // some example POJO
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User {
        private String name;

        private Integer score;

        private Instant event_time;
    }


    /**
     * 测试pojo,这个类型是immutable，即创建对象后，属性不能修改
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private String name;

        private Integer score;

        private Instant event_time;
    }
}
