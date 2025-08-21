package cn.hedeoer.chaptor06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// letf join
/*
* 左流驱动 (Left-driven): LEFT JOIN 的所有输出都是由左表的数据流触发的。右表的数据流只能被动地等待匹配或更新已有的结果，它自己无法独立产生输出
*
* 1. 默认情况下，必须设置状态的TTL，否则会导致状态的无限增大
*
* 2. 在设置了TTL的情况下，采用的策略默认为OnCreateAndWrite，即只有在状态的首次创建和状态的更新时，该条记录的TTL才会重置；另一种策略为OnReadAndWrite，
* 即涉及状态的读取和更新，该条记录的TTL都会重置，这在某些场景有用，flink sql默认的策略为OnCreateAndWrite，且无法改为OnReadAndWrite
*
* 3. left join时 两表数据到达顺序对结果的影响
 * +--------+------------------------------------------+-------------------------------------------------------------------------------------------------+------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
 * | 场景   | 事件顺序 (Event Order)                   | 内部状态变化 (Internal State Change)                                                            | 输出的变更日志流 (Changelog Stream)            | 核心解释 (Core Explanation)                                                                                                                                                |
 * +--------+------------------------------------------+-------------------------------------------------------------------------------------------------+------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
 * | 1      | 左先进，匹配的右后到                     | 1. `t1` 记录到达，被存入左表状态。                                                                | // t1 到达时                                   | 因为是 `LEFT JOIN`，左表数据一到，必须立即产出一个结果 `(t1, NULL)`。当匹配的右表数据                                                                                      |
 * |        | (最典型场景)                             | 2. 匹配的 `t2` 记录到达，在左表状态中找到 `t1`，然后 `t2` 被存入右表状态。                            | +I (t1_data, NULL)                             | 后来到达时，之前的 `(t1, NULL)` 结果已过时，所以 Flink 必须先撤回 (`-D`) 旧结果，                                                                                    |
 * |        |                                          |                                                                                                 |                                                | 再发出 (`+I`) 新的、连接成功的结果。                                                                                                                                     |
 * |        |                                          |                                                                                                 | // t2 到达时                                   |                                                                                                                                                                            |
 * |        |                                          |                                                                                                 | -D (t1_data, NULL)                             |                                                                                                                                                                            |
 * |        |                                          |                                                                                                 | +I (t1_data, t2_data)                          |                                                                                                                                                                            |
 * +--------+------------------------------------------+-------------------------------------------------------------------------------------------------+------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
 * | 2      | 右先进，匹配的左后到                     | 1. `t2` 记录到达，在左表状态中未找到匹配，于是被存入右表状态等待。                                  | // t2 到达时                                   | `LEFT JOIN` 的输出是由左表事件驱动的。仅有右表数据到达时，不满足输出条件，算子                                                                                               |
 * |        |                                          | 2. 匹配的 `t1` 记录到达，在右表状态中找到 `t2`，然后 `t1` 被存入左表状态。                            | (无任何输出)                                   | 没有任何输出，只是将 `t2` 缓存到状态中。直到驱动事件（`t1` 数据）到达，才将缓存                                                                                         |
 * |        |                                          |                                                                                                 |                                                | 数据进行匹配，并一次性输出连接成功的结果。                                                                                                                               |
 * |        |                                          |                                                                                                 | // t1 到达时                                   |                                                                                                                                                                            |
 * |        |                                          |                                                                                                 | +I (t1_data, t2_data)                          |                                                                                                                                                                            |
 * +--------+------------------------------------------+-------------------------------------------------------------------------------------------------+------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
 * | 3      | 左先进，永无匹配                         | 1. `t1` 记录到达，被存入左表状态。                                                                | // t1 到达时                                   | 左表数据到达立即输出 `(t1, NULL)`。此后，如果在状态保留时间 (TTL) 内一直没有匹配的                                                                                       |
 * |        | (或匹配项在TTL后才到)                    | 2. `t1` 记录的状态因 TTL 过期而被清除。                                                           | +I (t1_data, NULL)                             | 右表数据到来，这个 `(t1, NULL)` 就是最终结果。状态被清除是一个内部优化动作，不会                                                                                         |
 * |        |                                          |                                                                                                 |                                                | 产生额外的输出。                                                                                                                                                         |
 * |        |                                          |                                                                                                 | // 状态过期时                                  |                                                                                                                                                                            |
 * |        |                                          |                                                                                                 | (无任何输出)                                   |                                                                                                                                                                            |
 * +--------+------------------------------------------+-------------------------------------------------------------------------------------------------+------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
 * | 4      | 右先进，永无匹配                         | 1. `t2` 记录到达，被存入右表状态。                                                                | // t2 到达时                                   | 右表数据到达时不会触发输出。如果它在状态中等待超过了 TTL 仍没有匹配的左表数据                                                                                                |
 * |        | (或匹配项在TTL后才到)                    | 2. `t2` 记录的状态因 TTL 过期而被清除。                                                           | (无任何输出)                                   | 到来，它就会被静默地从状态中清除，以防止内存泄漏。整个过程不会产生任何输出。                                                                                                   |
 * |        |                                          |                                                                                                 |                                                |                                                                                                                                                                            |
 * |        |                                          |                                                                                                 | // 状态过期时                                  |                                                                                                                                                                            |
 * |        |                                          |                                                                                                 | (无任何输出)                                   |                                                                                                                                                                            |
 * +--------+------------------------------------------+-------------------------------------------------------------------------------------------------+------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
 */
public class $05LeftJoinDealWithPostCommentBySQL {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为了方便观察，并行度设为1
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //
        tableEnv.getConfig().getConfiguration().setString("table.exec.state.ttl", "5 s");

        TableResult tableResult1 = tableEnv.executeSql("CREATE TABLE users (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    email STRING,\n" +
                "    phone STRING,\n" +
                "    website STRING,\n" +
                "    city STRING,\n" +
                "    company STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'mz_datagen_blog_users',\n" +
                "    'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "    'properties.group.id' = 'flink-consumer-group-1',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json'\n" +
                ")");

        TableResult tableResult2 = tableEnv.executeSql("CREATE TABLE posts (\n" +
                "    id INT,\n" +
                "    user_id INT,\n" +
                "    title STRING,\n" +
                "    `ts` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '0' SECOND,\n" +
                "    body STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'mz_datagen_blog_posts',\n" +
                "    'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "    'properties.group.id' = 'flink-consumer-group-2',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json'\n" +
                ")");


        TableResult tableResult3 = tableEnv.executeSql("CREATE TABLE comments (\n" +
                "    id INT,\n" +
                "    user_id INT,\n" +
                "    body STRING,\n" +
                "    post_id INT,\n" +
                "    `views` INT,\n" +
                "    `ts` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '0' SECOND,\n" +
                "    status INT\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'mz_datagen_blog_comments',\n" +
                "    'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "    'properties.group.id' = 'flink-consumer-group-3',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json'\n" +
                ")");

/*
select
    t1.id as post_id,
    t1.title as post_title,
    t2.id as comment_id,
    t2.body as comment_body
from posts t1
left join comments t2
on t1.id = t2.post_id


* */
        tableEnv.executeSql(
                "select\n" +
                        "    t1.id as post_id,\n" +
                        "    t1.title as post_title,\n" +
                        "    t2.id as comment_id,\n" +
                        "    t2.body as comment_body\n" +
                        "from posts t1\n" +
                        "left join comments t2\n" +
                        "on t1.id = t2.post_id").print();


        // 7. 启动作业
        env.execute("User Behavior View-Then-Favorite Analysis");
    }
}
