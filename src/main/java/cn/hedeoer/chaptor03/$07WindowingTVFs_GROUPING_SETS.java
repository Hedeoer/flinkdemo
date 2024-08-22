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

// 累计窗口，窗口大小必须为滑动步长的整数倍
public class $07WindowingTVFs_GROUPING_SETS {
    public static void main(String[] args) throws Exception {

        // 创建配置对象
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 65535); // 设置Web UI端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<TaxiFare> eventStream = env
                .addSource(new TaxiFareGenerator())
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                                .withTimestampAssigner((e,t) -> e.getEventTimeMillis() ));




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
//        tableEnv.executeSql("select * from driverFare").print();





        // GROUPING SETS 使用
        tableEnv.executeSql(
                "select " +
                        "  window_start,window_end, paymentType, count(1)" +
                        " from table(tumble(table driverFare , DESCRIPTOR(eventTime), INTERVAL '5' MINUTES))" +
                        " group by window_start,window_end,GROUPING SETS ((paymentType), ()) "
        ).print();

        //+----+-------------------------+-------------------------+--------------------------------+----------------------+
        //| op |            window_start |              window_end |                    paymentType |               EXPR$3 |
        //+----+-------------------------+-------------------------+--------------------------------+----------------------+
        //| +I | 2020-01-01 12:00:00.000 | 2020-01-01 12:05:00.000 |                           CASH |                    7 |
        //| +I | 2020-01-01 12:00:00.000 | 2020-01-01 12:05:00.000 |                         (NULL) |                   14 |
        //| +I | 2020-01-01 12:00:00.000 | 2020-01-01 12:05:00.000 |                           CARD |                    7 |

        // rollup使用

        // CUBE 使用


        env.execute();
    }
}
