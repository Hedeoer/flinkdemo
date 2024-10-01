package cn.hedeoer.chapter01.physicalpartitioning;


import cn.hedeoer.common.datatypes.DataModel;
import cn.hedeoer.common.sources.MyAcSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 将数据均匀地随机分配到所有下游并行子任务中，确保每个子任务处理的数据量相对均衡。
public class $05Rebalance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        env.addSource(new MyAcSource()).rebalance()
                .map(new RichMapFunction<DataModel, DataModel>() {
                    @Override
                    public DataModel map(DataModel value) throws Exception {
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        value.setTransSubTaskName("TransTask" + indexOfThisSubtask);
                        return value;
                    }
                }).setParallelism(4)
                .print();

        env.execute();
    }
}
