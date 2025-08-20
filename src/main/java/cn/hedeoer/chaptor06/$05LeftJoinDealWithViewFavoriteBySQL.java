package cn.hedeoer.chaptor06;

import cn.hedeoer.common.datatypes.Favorite;
import cn.hedeoer.common.datatypes.ProductView;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

// todo
public class $05LeftJoinDealWithViewFavoriteBySQL {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为了方便观察，并行度设为1
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(60));
        tableEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "5 s");

        // 2. 创建数据源并解析为实体类

        // 2.1 用户浏览商品流 (从9999端口)
        // 输入格式: userId,productId,sessionId,viewTime(毫秒时间戳)
        // 例如: user1,productA,session01,1723200000000
        DataStream<String> viewSocketStream = env.socketTextStream("localhost", 9999);
        DataStream<ProductView> viewStream = viewSocketStream.map(line -> {
            String[] parts = line.split(",");
            return new ProductView(parts[0], parts[1], parts[2], Long.parseLong(parts[3]));
        });

        // 2.2 用户收藏商品流 (从8888端口)
        // 输入格式: userId,productId,favoriteTime(毫秒时间戳)
        // 例如: user1,productA,1723200060000
        DataStream<String> favoriteSocketStream = env.socketTextStream("localhost", 8888);
        DataStream<Favorite> favoriteStream = favoriteSocketStream.map(line -> {
            String[] parts = line.split(",");
            return new Favorite(parts[0], parts[1], Long.parseLong(parts[2]));
        });


        // 3. 定义事件时间和Watermark策略
        // 我们允许2秒的乱序
        WatermarkStrategy<ProductView> viewWatermarkStrategy = WatermarkStrategy
                .<ProductView>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> event.viewTime);

        WatermarkStrategy<Favorite> favoriteWatermarkStrategy = WatermarkStrategy
                .<Favorite>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> event.favoriteTime);

        DataStream<ProductView> viewStreamWithWatermarks = viewStream.assignTimestampsAndWatermarks(viewWatermarkStrategy);
        DataStream<Favorite> favoriteStreamWithWatermarks = favoriteStream.assignTimestampsAndWatermarks(favoriteWatermarkStrategy);


        // 4. 将 DataStream 转换为 Table, 并定义 Schema
        // 关键：将事件时间戳字段转换为 Flink SQL 的 TIMESTAMP_LTZ 类型
        Table viewTable = tableEnv.fromDataStream(viewStreamWithWatermarks,
                Schema.newBuilder()
                        .column("userId", "STRING")
                        .column("productId", "STRING")
                        .column("sessionId", "STRING")
                        .columnByExpression("view_ts", "TO_TIMESTAMP_LTZ(viewTime, 3)") // 从毫秒转为时间戳
                        .watermark("view_ts", "view_ts - INTERVAL '2' SECOND") // 定义Watermark
                        .build());

        Table favoriteTable = tableEnv.fromDataStream(favoriteStreamWithWatermarks,
                Schema.newBuilder()
                        .column("userId", "STRING")
                        .column("productId", "STRING")
                        .columnByExpression("fav_ts", "TO_TIMESTAMP_LTZ(favoriteTime, 3)")
                        .watermark("fav_ts", "fav_ts - INTERVAL '2' SECOND")
                        .build());

        // 注册临时视图
        tableEnv.createTemporaryView("ProductView", viewTable);
        tableEnv.createTemporaryView("Favorite", favoriteTable);


        // 5. 编写并执行 INTERVAL LEFT JOIN SQL
        String sqlQuery =
                "SELECT " +
                        "    v.userId, " +
                        "    v.productId, " +
                        "    v.view_ts, " +
                        "    CAST(f.fav_ts AS TIMESTAMP) AS favorite_time, " +
                        "    CASE WHEN f.userId IS NOT NULL THEN 'Yes' ELSE 'No' END AS is_favorited " +
                        "FROM ProductView AS v " +
                        "LEFT JOIN Favorite AS f ON v.userId = f.userId AND v.productId = f.productId " +
                        // **核心：时间边界条件**
                        // 收藏时间必须在浏览时间之后，并且在浏览后的1小时之内
                        "AND f.fav_ts BETWEEN v.view_ts AND v.view_ts + INTERVAL '1' HOUR";

        Table resultTable = tableEnv.sqlQuery(sqlQuery);

        // 6. 将结果打印到控制台
        tableEnv.toChangelogStream(resultTable).print();

        // 7. 启动作业
        env.execute("User Behavior View-Then-Favorite Analysis");
    }
}
