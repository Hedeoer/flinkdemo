package cn.hedeoer.chaptor03;

import cn.hedeoer.common.datatypes.VideoPlayProgress;
import cn.hedeoer.common.sources.VideoPlayProgressSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 适用场景：是temporal join在特定场景下的平替；当版本表的数据非常巨大，不宜全量加载入内存时，可以考虑适用lookup join
 * 1. 适用要求
 * 1.1 只能应用在处理时间的流上
 * 1.2 版本表存储有相应的connector
 * 2. 注意事项
 * 2.1 主表每来一条数据，都会触发一次请求版本表的请求，对外面数据库来说，频繁大量的处理请求是灾难性的，应该考虑查询的数据缓存，查询的异步请求等手段平衡
 * 2.2 temporal join和lookup join语法是一致的
 */
public class $14LookUpJoin {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 视频播放进度日志流
        DataStreamSource<VideoPlayProgress> processSource = env.addSource(new VideoPlayProgressSource());


        // 视频维度表，来自mysql
        TableResult videoDimResult = tableEnv.executeSql("CREATE TABLE video_info (\n" +
                "    id BIGINT COMMENT '编号',\n" +
                "    video_name STRING COMMENT '视频名称',\n" +
                "    during_sec BIGINT COMMENT '时长',\n" +
                "    video_status STRING COMMENT '状态 未上传，上传中，上传完',\n" +
                "    video_size BIGINT COMMENT '大小',\n" +
                "    video_url STRING COMMENT '视频存储路径',\n" +
                "    video_source_id STRING COMMENT '云端资源编号',\n" +
                "    version_id BIGINT COMMENT '版本号',\n" +
                "    chapter_id BIGINT COMMENT '章节id',\n" +
                "    course_id BIGINT COMMENT '课程id',\n" +
                "    publisher_id BIGINT COMMENT '发布者id',\n" +
                "    create_time TIMESTAMP COMMENT '创建时间',\n" +
                "    update_time TIMESTAMP COMMENT '更新时间',\n" +
                "    deleted STRING COMMENT '是否删除',\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://hadoop102:3306/edu?useSSL=false',\n" +
                "    'table-name' = 'video_info',\n" +
                "    'driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'aaaaaa'\n" +
                ")\n");
//        tableEnv.executeSql("select * from video_info").print();

        Table processTable = tableEnv.fromDataStream(processSource, Schema.newBuilder()
                .column("videoId", DataTypes.BIGINT())
                .column("playSec", DataTypes.BIGINT())
                .column("positionSec", DataTypes.BIGINT())
                .columnByExpression("evt", "PROCTIME()")
                .build());

        tableEnv.createTemporaryView("process_play", processTable);
//        tableEnv.executeSql("select *  from process_table").print();

        tableEnv.executeSql("select\n" +
                "t1.videoId,\n" +
                "t1.playSec,\n" +
//                "t2.evt,\n" +
                "t2.video_name,\n" +
                "t2.during_sec\n" +
                "from process_play as t1\n" +
                "join video_info for system_time as of t1.evt as t2\n" +
                "on t1.videoId = t2.id").print();

//        env.execute("flink sql task");


    }
}
