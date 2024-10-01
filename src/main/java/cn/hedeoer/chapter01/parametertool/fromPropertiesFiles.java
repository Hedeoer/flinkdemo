package cn.hedeoer.chapter01.parametertool;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 可以从配置文件获取
 */
public class fromPropertiesFiles {

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
