package cn.hedeoer.chapter01.physicalpartitioning;

import cn.hedeoer.common.datatypes.DataModel;
import cn.hedeoer.common.sources.MyAcSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 当数据必须保持严格顺序处理，且上下游并行度相同时
public class $08Forward {
    public static void main(String[] args) throws Exception {
        // 创建一个 Flink Configuration 对象
        Configuration config = new Configuration();

        // 设置 Flink Web UI 的端口，默认为 8081
        config.setString("rest.port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(config);
        env.setParallelism(2);

        env.addSource(new MyAcSource())
                .map(new RichMapFunction<DataModel, DataModel>() {
                    @Override
                    public DataModel map(DataModel value) throws Exception {
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        value.setTransSubTaskName("TransTask" + indexOfThisSubtask);
                        return value;
                    }
                })
                .print().disableChaining();

        env.execute();
    }
}
