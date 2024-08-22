package cn.hedeoer.chaptor03;


import cn.hedeoer.common.datatypes.TaxiRide;
import cn.hedeoer.common.sources.TaxiRideGenerator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *数组炸裂
 */
public class $15ArrayExpansion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<TaxiRide> source = env.addSource(new TaxiRideGenerator());
        StreamTableEnvironment tableEnc = StreamTableEnvironment.create(env);
        Table rideTable = tableEnc.fromDataStream(source, Schema.newBuilder()
                .column("rideId", DataTypes.BIGINT())
                .column("passengerNames", DataTypes.ARRAY(DataTypes.STRING()))
                .build());

        tableEnc.createTemporaryView("ride", rideTable);
        tableEnc.executeSql("SELECT rideId, passengerName\n" +
                "FROM ride CROSS JOIN UNNEST(passengerNames) AS t (passengerName)").print();
        //+----+----------------------+--------------------------------+
        //| op |               rideId |                  passengerName |
        //+----+----------------------+--------------------------------+
        //| +I |                    5 |                         Xavier |
        //| +I |                    5 |                          Kevin |
        //| +I |                    5 |                          Alice |
        //| +I |                    5 |                          Quinn |
        //| +I |                    3 |                         Victor |
        //| +I |                    3 |                          Quinn |
        //| +I |                    3 |                         Rachel |

//        source.executeAndCollect();
        env.execute("flink sql task");
    }
}
