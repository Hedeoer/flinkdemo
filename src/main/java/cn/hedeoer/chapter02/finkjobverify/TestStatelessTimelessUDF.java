package cn.hedeoer.chapter02.finkjobverify;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;


// 测试无状态，无时间处理相关的算子
public class TestStatelessTimelessUDF {

    // 通过创建算子实例，调用算子方法验证逻辑是否正确，对应的test类，查看TestStatelessTimelessUDFTest
    public static class IncrementMapFunction implements MapFunction<Long, Long> {

        @Override
        public Long map(Long record) throws Exception {
            return record + 1;
        }
    }

    public static class IncrementFlatMapFunction implements FlatMapFunction<Long, Long>{
        @Override
        public void flatMap(Long value, Collector<Long> out) throws Exception {
            out.collect(value + 1);
        }
    }
}
