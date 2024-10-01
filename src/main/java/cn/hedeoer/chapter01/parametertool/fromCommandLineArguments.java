package cn.hedeoer.chapter01.parametertool;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 通过ParameterTool获取命令行传入flink程序的参数，这样如果要修改某个算子的并行度，或者flink程序使用了外面存储系统的一些配置，可以通过ParameterTool解析参数，
 * 而不用下线，修改程序并提交任务
 * 比如 --name hedeoer --age 18
 */
public class fromCommandLineArguments {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("hello")
                        .map(new MapFunction<String, String>() {
                            @Override
                            public String map(String value) throws Exception {
                                ParameterTool parameterTool = ParameterTool.fromArgs(args);
                                String name = parameterTool.get("name");
                                String age = parameterTool.get("age");
                                return value + " " + name + " " + age;
                            }
                        })
                                .print();


        env.execute();
    }
}
