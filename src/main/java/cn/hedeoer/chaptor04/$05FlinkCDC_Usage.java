package cn.hedeoer.chaptor04;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用flink原生的connector处理：
 * 使用table api connectors的JDBC SQL Connector读取mysql的数据，处理之后，写入kafka(普通的kafka)中
 * MySQL的原表：video_info，视频维度表，只有数据增长的情况， 没有删除和更新的情况，故不需要使用upsert kafka
 */
public class $05FlinkCDC_Usage {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



        // 视频维度表，来自mysql
        tableEnv.executeSql("CREATE TABLE video_info (\n" +
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
//        tableEnv.executeSql("desc  table video_info").print();

        // 写入数据到kafka
        tableEnv.executeSql("CREATE TABLE ods_video_info (\n" +
                "  `partition` BIGINT METADATA VIRTUAL,\n" +
                "  `offset` BIGINT METADATA VIRTUAL,\n" +
                "  id BIGINT COMMENT '编号',\n" +
                "  video_name STRING COMMENT '视频名称',\n" +
                "  during_sec BIGINT COMMENT '时长',\n" +
                "  video_status STRING COMMENT '状态 未上传',\n" +
                "  video_size BIGINT COMMENT '大小',\n" +
                "  video_url STRING COMMENT '视频存储路径',\n" +
                "  video_source_id STRING COMMENT '云端资源编号',\n" +
                "  version_id BIGINT COMMENT '版本号',\n" +
                "  chapter_id BIGINT COMMENT '章节id',\n" +
                "  course_id BIGINT COMMENT '课程id',\n" +
                "  publisher_id BIGINT COMMENT '发布者id',\n" +
                "  create_time TIMESTAMP COMMENT '创建时间',\n" +
                "  update_time TIMESTAMP COMMENT '更新时间',\n" +
                "  deleted STRING COMMENT '是否删除'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flink_sql',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true',\n" +
                "  'sink.partitioner' = 'fixed'\n" +
                ")\n");

        tableEnv.executeSql("insert into ods_video_info \n" +
                "select\n" +
                "id ,\n" +
                "video_name ,\n" +
                "during_sec ,\n" +
                "video_status ,\n" +
                "video_size ,\n" +
                "video_url ,\n" +
                "video_source_id ,\n" +
                "version_id ,\n" +
                "chapter_id ,\n" +
                "course_id ,\n" +
                "publisher_id ,\n" +
                "create_time ,\n" +
                "update_time ,\n" +
                "deleted \n" +
                "from video_info");

        tableEnv.executeSql("select count(1) from ods_video_info").print();
        //| -U |                 5403 |
        //| +U |                 5404 |
        //| -U |                 5404 |
        //| +U |                 5405 |
        //| -U |                 5405 |
        //| +U |                 5406 |
        //| -U |                 5406 |
        //| +U |                 5407 |
        //| -U |                 5407 |
        //| +U |                 5408 |
    }
}
