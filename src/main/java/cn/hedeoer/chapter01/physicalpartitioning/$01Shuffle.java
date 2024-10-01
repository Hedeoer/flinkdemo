package cn.hedeoer.chapter01.physicalpartitioning;

import cn.hedeoer.common.datatypes.DataModel;
import cn.hedeoer.common.sources.MyAcSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 全局打乱数据顺序，适用于全局排序等场景
public class $01Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        env.addSource(new MyAcSource()).shuffle()
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
