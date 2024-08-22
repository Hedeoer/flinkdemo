package cn.hedeoer.common.utils;

import org.apache.calcite.util.Static;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class SinkUtil {


    public static FileSink<String> getFileSink(String path)   {

        SinkUtil.deleteContentDirectory(path);
        final FileSink<String> sink = FileSink
                .forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(150000))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(500000))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        return sink;
    }

    private static void deleteContentDirectory(String path)   {
        // 删除path下的所有文件目录
        try {
            FileUtils.deleteDirectory(new File(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) {
        SinkUtil.deleteContentDirectory("output/taxiRideStream");
    }



}
