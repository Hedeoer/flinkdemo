package cn.hedeoer.chaptor03;



import cn.hedeoer.common.datatypes.TaxiFare;
import cn.hedeoer.common.sources.TaxiFareGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/*
*开窗函数 over的使用
* 1. 格式
* SELECT
  agg_func(agg_col) OVER (
    [PARTITION BY col1[, col2, ...]]
    ORDER BY time_col
    range_definition),
  ...
FROM ...
*
* 2. 范围查询的类型
* 2.1 基于行数
* 2.2 基于时间范围
*
* 3. partition by 子句是可选的
*
* 4. over子句中必须有 order by 子句，原因flink table 数据本身没有数据可言，必须使用order  by 指定排序字段来排序；并且只能时升序排列
*
* 5.使用记录数范围类型时，bettwen and current row,目前只能使用current row
* */
public class $09OverAggregation {

    /**
     * 计算每个司机过去30分钟内截止每次行程的总收入（相当于每次行程都要计算过去30分钟的总收入，累计求和的效果）
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // 创建配置对象
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 65535); // 设置Web UI端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        // 读取数据源，并分配时间戳、
        SingleOutputStreamOperator<TaxiFare> eventStream = env
                .addSource(new TaxiFareGenerator())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                        .withTimestampAssigner((e, t) -> e.getEventTimeMillis()));


        SingleOutputStreamOperator<Tuple5> covertedStream = eventStream.map(fare -> new Tuple5(fare.rideId, fare.driverId, fare.paymentType, fare.totalFare, fare.startTime.toEpochMilli()))
                .returns(Types.TUPLE(Types.LONG, Types.LONG, Types.STRING, Types.FLOAT, Types.LONG));


        Table table = tableEnv.fromDataStream(covertedStream,
                $("f0").as("rideId"),
                $("f1").as("driverId"),
                $("f2").as("paymentType"),
                $("f3").as("totalFare"),
                $("f4").rowtime().as("eventTime"));

        // 注册临时表1,使用流记录中字段作为事件时间
        tableEnv.createTemporaryView("driverFare", table);




        tableEnv.executeSql("select\n" +
                "\tdriverId,\n" +
                "\ttotalFare,\n" +
                "\teventTime,\n" +
                "\tsum(totalFare) \n" +
                "\tover(PARTITION BY driverId order by eventTime\n" +
                "\tRANGE BETWEEN interval '30' MINUTE preceding AND CURRENT ROW)\n" +
                "from driverFare ").print();



        env.execute();
    }
}
