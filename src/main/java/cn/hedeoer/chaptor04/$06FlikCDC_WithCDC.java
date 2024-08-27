package cn.hedeoer.chaptor04;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// 使用flink cdc 平替 cn.hedeoer.chaptor04.$05FlinkCDC_Usage的实现

/**
 * 考虑到mysql的数据有删除更新操作
 * 同步mysql的数据到kafka
 * 目前测试使用的flink cdc版本为3.1.1，
 * flink版本为1.13.6
 * <p>
 * 1. 开启checkpoint，生产建议5到10分钟
 * 2. cdc source并行度设置，如何需要增量快照的功能，需要server-id的范围大于并行度
 * 3. 使用upsert kafka，必须指定primary key，key和value的格式
 * 4. 启动之后mysql的数据变化都会立即反映到kafka
 */
public class $06FlikCDC_WithCDC {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // Set configurations
        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.getConfiguration().setString("execution.checkpointing.interval", "3s");
        tableConfig.getConfiguration().setString("parallelism.default", "11");


        tableEnv.executeSql(
                "CREATE TABLE video_info (" +
                        "    db_name STRING METADATA FROM 'database_name' VIRTUAL," +
                        "    table_name STRING METADATA FROM 'table_name' VIRTUAL," +
                        "    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL," +
                        "    id BIGINT COMMENT '编号'," +
                        "    video_name STRING COMMENT '视频名称'," +
                        "    during_sec BIGINT COMMENT '时长'," +
                        "    video_status STRING COMMENT '状态 未上传'," +
                        "    video_size BIGINT COMMENT '大小'," +
                        "    video_url STRING COMMENT '视频存储路径'," +
                        "    video_source_id STRING COMMENT '云端资源编号'," +
                        "    version_id BIGINT COMMENT '版本号'," +
                        "    chapter_id BIGINT COMMENT '章节id'," +
                        "    course_id BIGINT COMMENT '课程id'," +
                        "    publisher_id BIGINT COMMENT '发布者id'," +
                        "    create_time TIMESTAMP COMMENT '创建时间'," +
                        "    update_time TIMESTAMP COMMENT '更新时间'," +
                        "    deleted STRING COMMENT '是否删除'," +
                        "    PRIMARY KEY (id) NOT ENFORCED" +
                        ") WITH (" +
                        "    'connector' = 'mysql-cdc'," +
                        "    'hostname' = 'hadoop102'," +
                        "    'port' = '3306'," +
                        "    'username' = 'root'," +
                        "    'password' = 'aaaaaa'," +
                        "    'database-name' = 'edu'," +
                        "    'table-name' = 'video_info'," +
                        "    'server-id' = '5400-5410'," +
                        "    'scan.incremental.snapshot.enabled' = 'true'," +
                        "    'server-time-zone' = 'Asia/Shanghai'" +
                        ")"
        );


//        tableEnv.executeSql("select count(1) from video_info").print();

        // 写入数据到kafka
        tableEnv.executeSql("CREATE TABLE ods_video_info ( " +
                "  `partition` BIGINT METADATA VIRTUAL, " +
                "  `offset` BIGINT METADATA VIRTUAL, " +
                "  id BIGINT COMMENT '编号', " +
                "  video_name STRING COMMENT '视频名称', " +
                "  during_sec BIGINT COMMENT '时长', " +
                "  video_status STRING COMMENT '状态 未上传', " +
                "  video_size BIGINT COMMENT '大小', " +
                "  video_url STRING COMMENT '视频存储路径', " +
                "  video_source_id STRING COMMENT '云端资源编号', " +
                "  version_id BIGINT COMMENT '版本号', " +
                "  chapter_id BIGINT COMMENT '章节id', " +
                "  course_id BIGINT COMMENT '课程id', " +
                "  publisher_id BIGINT COMMENT '发布者id', " +
                "  create_time TIMESTAMP COMMENT '创建时间', " +
                "  update_time TIMESTAMP COMMENT '更新时间', " +
                "  deleted STRING COMMENT '是否删除' , " +
                "  PRIMARY KEY (`id`) NOT ENFORCED" +
                ") WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = 'flink_sql', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'properties.group.id' = 'testGroup', " +
                "'key.format' = 'json'," +
                "'key.json.ignore-parse-errors' = 'true'," +
                "'value.format' = 'json'," +
                "'value.json.fail-on-missing-field' = 'false'," +
                "'value.fields-include' = 'EXCEPT_KEY'" +


                ") ");


        tableEnv.executeSql("insert into ods_video_info  " +
                "select " +
                "id , " +
                "video_name , " +
                "during_sec , " +
                "video_status , " +
                "video_size , " +
                "video_url , " +
                "video_source_id , " +
                "version_id , " +
                "chapter_id , " +
                "course_id , " +
                "publisher_id , " +
                "create_time , " +
                "update_time , " +
                "deleted  " +
                "from video_info");
        tableEnv.executeSql("select count(1) from ods_video_info").print();
        //| +U |                 5408 |
        //| -U |                 5408 |
        //| +U |                 5407 |
        //| -U |                 5407 |
        //| +U |                 5406 |
        //| -U |                 5406 |
        //| +U |                 5407 |


    }
}
