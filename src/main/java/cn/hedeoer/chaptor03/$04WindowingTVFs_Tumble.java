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

// tvfs的 表值函数， 返回值自带 window_start,window_end, window_time 三个属性， 其中window_time = window_end -1；
import static org.apache.flink.table.api.Expressions.$;

public class $04WindowingTVFs_Tumble {
    public static void main(String[] args) throws Exception {

        // 创建配置对象
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 65535); // 设置Web UI端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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





        tableEnv.executeSql(
                "select " +
                        "  window_start,window_end, count(1)" +
                        " from table(tumble(table driverFare , DESCRIPTOR(eventTime), interval '66' second))" +
                        " group by window_start,window_end "
        ).print();

        env.execute();
    }
}
