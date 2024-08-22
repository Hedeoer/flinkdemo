package cn.hedeoer.chaptor03;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * dataStream 和 table的整合，转化
 * EXAMPLE 1: DataStream to Table,直接将数据流转化为table
 * EXAMPLE 2: 为数据流添加处理时间
 * EXAMPLE 3: 为数据流添加处理时间，并指定水印策略
 * EXAMPLE 4: 如果流中已经指定了水印的策略，那么转化为表时，只需要从元数据中获取即可
 * EXAMPLE 5: 直接手动指定列名，并做一些列类型范围的转化，比如TIMESTAMP_LTZ(9)转化为TIMESTAMP_LTZ(3)，注意列的顺序和实体类属性顺序不同和相同两种情况下的区别
 */
public class $00DataStreamIntegration {

    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tableEnv;
    private static DataStream<User> dataStream;
    private static DataStream<Student> studentDataStream;

    @BeforeAll
    public static void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnv = StreamTableEnvironment.create(env);
        // create a DataStream
        dataStream =
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
                        new Student("Alice", 10, Instant.ofEpochMilli(1002)))
                        // 指定水印生成和提取策略
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Student>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((element, timestamp) -> element.event_time.toEpochMilli()));

    }

    /**
     * 对于只有数据不断插入的数据流，转化为table 的情况
     * 1. todo 1 只有append情况的流转化为表
     */
    @Test
    public void InsertOnlyStream() {

        // === EXAMPLE 1 ===

        // derive all physical columns automatically
        Table table = tableEnv.fromDataStream(dataStream);
        table.printSchema();
        //(
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9)
        //)


        // === EXAMPLE 2 ===

        // derive all physical columns automatically
        // but add computed columns (in this case for creating a proctime attribute column)
        // 指定处理时间列
        Table table1 = tableEnv.fromDataStream(
                dataStream,
                Schema.newBuilder()
                        .columnByExpression("proc_time_self", "PROCTIME()")
                        .build());
        table1.printSchema();

        //(
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9),
        //  `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
        //)

        // === EXAMPLE 3 ===

        // derive all physical columns automatically
        // but add computed columns (in this case for creating a rowtime attribute column)
        // and a custom watermark strategy
        // 通过列的计算指定水印和水印策略
        Table table2 =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
                                .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                                .build());
        table2.printSchema();

        //(
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9),
        //  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
        //  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
        //)

        // === EXAMPLE 4 ===

        // derive all physical columns automatically
        // but access the stream record's timestamp for creating a rowtime attribute column
        // also rely on the watermarks generated in the DataStream API

        // we assume that a watermark strategy has been defined for `dataStream` before
        // dataStream已经有水印了，此时流转化为表需要从元数据获取即可
        Table table3 =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                                .watermark("rowtime", "SOURCE_WATERMARK()")
                                .build());
        table3.printSchema();
        //(
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9),
        //  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
        //  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
        //)

        // === EXAMPLE 5 ===

        // define physical columns manually
        // in this example,
        //   - we can reduce the default precision of timestamps from 9 to 3
        //   - we also project the columns and put `event_time` to the beginning

        Table table4 =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column("event_time", "TIMESTAMP_LTZ(3)")
                                .column("name", "STRING")
                                .column("score", "INT")
                                .watermark("event_time", "SOURCE_WATERMARK()")
                                .build());
        table4.printSchema();

        // note: the watermark strategy is not shown due to the inserted column reordering projection
        //(
        //  `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
        //  `name` STRING,
        //  `score` INT
        //)

        // otherwise,if declare column order is same as pojo field order, the watermark strategy will be shown
        //(
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
        //  WATERMARK FOR `event_time`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
        //)



    }

    /**
     * 对于包含复杂数据结构的数据流，转化为table 的情况
     * 1. todo 2 只有append情况的流转化为表，且字段是复合数据类型
     */
    @Test
    public void InsertOnlyStreamForComplexDataStructure() {

        // 对于pojo是immutable的，每条流记录只能是ROW类型，列名为f0
        Table table1 = tableEnv.fromDataStream(studentDataStream);
        table1.printSchema();
        //(
        //  `f0` RAW('cn.hedeoer.chaptor03.$00DataStreamIntegration$Student', '...')
        //)

        // instead, declare a more useful data type for columns using the Table API's type system
        // in a custom schema and rename the columns in a following `as` projection

        Table table2 = tableEnv
                .fromDataStream(
                        studentDataStream,
                        Schema.newBuilder()
                                .column("f0", DataTypes.of(Student.class))
                                .build())
                .as("student");
        table2.printSchema();

        //(
        //  `student` *cn.hedeoer.chaptor03.$00DataStreamIntegration$Student<`name` STRING, `score` INT, `event_time` TIMESTAMP_LTZ(9)>*
        //)

        // data types can be extracted reflectively as above or explicitly defined

        Table table3 = tableEnv
                .fromDataStream(
                        studentDataStream,
                        Schema.newBuilder()
                                .column(
                                        "f0",
                                        DataTypes.STRUCTURED(
                                                Student.class,
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("score", DataTypes.INT())))
                                .build())
                .as("student");
        table3.printSchema();
        //(
        //  `student` *cn.hedeoer.chaptor03.$00DataStreamIntegration$Student<`name` STRING, `score` INT>*
        //)


    }

    /**
     * 针对only-appendly的数据流，测试创建临时视图
     * todo 3 测试创建临时视图
     */
    @Test
    public void InsertOnlyStreamForCreateTemporaryView () {
        // create some DataStream
        DataStream<Tuple2<Long, String>> dataStream = env.fromElements(
                Tuple2.of(12L, "Alice"),
                Tuple2.of(0L, "Bob"));

        // 可以为字段指定别名
        tableEnv.createTemporaryView(
                "MyView",
                tableEnv.fromDataStream(dataStream).as("id", "name"));

        tableEnv.from("MyView").printSchema();
        //(
        //  `id` BIGINT NOT NULL,
        //  `name` STRING
        //)

    }

    /**
     * todo 4 只有append情况的表转化为流
     * 只有不断插入，没有更新和删除的特征的表才能直接使用toDataStream方法转化为流
     * 其他情况考虑toChangelogStream方法
     */
    @Test
    public void InsertOnlyStreamForToDataStream () throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE GeneratedTable "
                        + "("
                        + "  name STRING,"
                        + "  score INT,"
                        + "  event_time TIMESTAMP_LTZ(3),"
                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
                        + ")"
                        + "WITH ('connector'='datagen')");

        Table table = tableEnv.from("GeneratedTable");

        // 直接使用toDataStream
        DataStream<Row> dataStream1 = tableEnv.toDataStream(table);
        // 通过实体类映射
        DataStream<User> dataStream2 = tableEnv.toDataStream(table, User.class);
        dataStream2.print();

        env.execute();


    }

    /**
     * todo 5 比较fromChangelogStream方法在reract流和upsert流中的使用区别
     * 操作符种类：在 retract 流中，更新操作需要 UPDATE_BEFORE 和 UPDATE_AFTER 成对出现，而在 upsert 流中，只有 UPDATE_AFTER 即可。
     * 处理逻辑：retract 流处理更复杂，可以处理没有主键的数据，而 upsert 流假设有主键，因此能更高效地处理更新操作。
     * 性能：upsert 流由于减少了操作符的数量，在拥有主键时性能更优，但 retract 流更加通用，适用性更广。
     */
    @Test
    public void changLogStream_fromChangelogStream(){
        // === EXAMPLE 1 ===

        // interpret the stream as a retract stream
        // 撤回流特定：如果流中存在更新，必须有UPDATE_BEFORE，UPDATE_AFTER成对出现

        // create a changelog DataStream
        DataStream<Row> dataStream =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

        // interpret the DataStream as a Table
        Table table = tableEnv.fromChangelogStream(dataStream);

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable", table);
        tableEnv
                .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
                .print();

        // prints:
        // +----+--------------------------------+-------------+
        // | op |                           name |       score |
        // +----+--------------------------------+-------------+
        // | +I |                            Bob |           5 |
        // | +I |                          Alice |          12 |
        // | -D |                          Alice |          12 |
        // | +I |                          Alice |         100 |
        // +----+--------------------------------+-------------+


        // === EXAMPLE 2 ===

        // interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)
        // upsert流特点：如果流中存在更新，只须有UPDATE_AFTER之后记录即可，相比回撤流，流的记录数要少一半
        // create a changelog DataStream
        DataStream<Row> dataStream1 =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

        // interpret the DataStream as a Table
        // 对于upsert流，指定主键性能更佳
        Table table1 =
                tableEnv.fromChangelogStream(
                        dataStream1,
                        Schema.newBuilder().primaryKey("f0").build(),
                        ChangelogMode.upsert());

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable1", table1);
        tableEnv
                .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable1 GROUP BY f0")
                .print();

        // prints:
        // +----+--------------------------------+-------------+
        // | op |                           name |       score |
        // +----+--------------------------------+-------------+
        // | +I |                            Bob |           5 |
        // | +I |                          Alice |          12 |
        // | -U |                          Alice |          12 |
        // | +U |                          Alice |         100 |
        // +----+--------------------------------+-------------+

    }

    /**
     * todo 6 将具有changlog类型的表转化为流
     * @throws Exception
     */
    @Test
    public void changLogStream_toChangelogStream () throws Exception {
        Table simpleTable = tableEnv
                .fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
                .as("name", "score")
                .groupBy($("name"))
                .select($("name"), $("score").sum());

        tableEnv
                .toChangelogStream(simpleTable)
                .executeAndCollect()
                .forEachRemaining(System.out::println);
        //+I[Bob, 12]
        //+I[Alice, 12]
        //-U[Alice, 12]
        //+U[Alice, 14]
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
    @AllArgsConstructor
    public static class Student {
        private final String name;

        private final Integer score;

        private final Instant event_time;
    }
}
