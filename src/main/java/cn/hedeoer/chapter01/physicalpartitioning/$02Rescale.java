package cn.hedeoer.chapter01.physicalpartitioning;


import cn.hedeoer.common.datatypes.DataModel;
import cn.hedeoer.common.sources.MyAcSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 一种优化的重平衡分区，数据按照上下游任务的并行度比例进行分配，使得数据尽量保持本地性(同一个taskmanager)。减少了不同taskmanager间的网络通信传输重新分配数据
// 区别与reblance
public class $02Rescale {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        env.addSource(new MyAcSource()).rescale()
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
