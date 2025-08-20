package cn.hedeoer.chaptor06;

import cn.hedeoer.common.datatypes.Order;
import cn.hedeoer.common.datatypes.Product;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Arrays;

/**
 * 描述: Flink SQL 左连接
 * 1. 当flink sql 中没有指定任何时间属性（事件时间，处理时间），默认使用处理时间
 * 2.
 */

public class $04LeftJoinBySQL {
    public static void main(String[] args) throws Exception {

        // 1. 初始化环境,并应用表状态设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 状态在被读取或写入时都会重置倒计时,如果超过 5秒内没有数据写入或读取,则状态会被清除
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 2. 创建数据源

        // 2.1 创建固定的商品维度数据源 (ProductDim)
        DataStream<Product> productStream = env.fromCollection(Arrays.asList(
                new Product(1L, "MacBook Pro", 15000.0),
                new Product(2L, "iPhone 15", 8000.0),
                new Product(3L, "iPad Air", 5000.0)
        ));

        // 2.2 创建从端口监听的订单流数据源 (OrderStream)
        // 使用 netcat (nc) 命令来发送数据: nc -lk 9999
        // 发送格式: 订单ID,商品ID,数量 (例如: 101,1,2)
        DataStream<String> socketStream = env.socketTextStream("localhost", 9999);

        // 将字符串流解析为Order对象流
        DataStream<Order> orderStream = socketStream.map(line -> {
            String[] parts = line.split(",");
            return new Order(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
        });


        // 3. 将 DataStream 转换为 Table
        Table productTable = tableEnv.fromDataStream(productStream);
        Table orderTable = tableEnv.fromDataStream(orderStream);

        // 为表注册临时视图，以便在SQL中使用
        tableEnv.createTemporaryView("ProductDim", productTable);
        tableEnv.createTemporaryView("OrderStream", orderTable);


        // 4. 执行 LEFT JOIN SQL 查询
        // 查询每个订单的详细信息，包括商品名和价格
        // 如果订单中的 productId 在商品表中不存在，productName 和 price 将为 null
        String sqlQuery = "SELECT " +
                "    o.orderId, " +
                "    o.productId, " +
                "    o.quantity, " +
                "    p.productName, " +
                "    p.price " +
                "FROM OrderStream AS o " +
                "LEFT JOIN ProductDim AS p ON o.productId = p.productId";

        Table resultTable = tableEnv.sqlQuery(sqlQuery);


        // 5. 将结果 Table 转换为 DataStream 并打印
        // 注意：对于流式查询，需要使用 toChangelogStream()
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

        resultStream.print();

        // 6. 启动 Flink 作业
        env.execute("Flink SQL Left Join Example");
    }

}
