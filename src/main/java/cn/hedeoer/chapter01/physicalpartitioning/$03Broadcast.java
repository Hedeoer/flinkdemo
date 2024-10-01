package cn.hedeoer.chapter01.physicalpartitioning;

import cn.hedeoer.common.datatypes.DataModel;
import cn.hedeoer.common.sources.MyAcSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//将数据复制并发送到所有下游并行子任务，每个子任务都接收到所有数据。
public class $03Broadcast {
    public static void main(String[] args) throws Exception {
        // 创建一个 Flink Configuration 对象
        Configuration config = new Configuration();

        // 设置 Flink Web UI 的端口，默认为 8081
        config.setString("rest.port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(config);
        env.setParallelism(2);


        env.addSource(new MyAcSource()).rescale()
                .broadcast()
                .map(new RichMapFunction<DataModel, DataModel>() {
                    @Override
                    public DataModel map(DataModel value) throws Exception {
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        value.setTransSubTaskName("TransTask" + indexOfThisSubtask);
                        return value;
                    }
                })
                .print();

        env.execute();
    }
}
