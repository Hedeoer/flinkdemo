package cn.hedeoer.chapter01.userdefinefunction;

import cn.hedeoer.taskconfig.StreamConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 * Accumulators 是一种用于在整个集群中汇总数据的机制。
 * 它们可以用于计算总和、计数、最小值、最大值等。Flink 提供了几种内置的累加器类型，如 LongCounter、DoubleCounter、Histogram 等
 *
 * 使用Accumulators和在算子内使用局部变量统计的区别：
 * 1. flink任务是分布式的，算子内的局部变量的值不同的task实例间共享，只能反映单个并行度实例的统计结果，而Accumulators是跨task实例共享的，可以反映整个任务统计结果
 * 2. 如果使用算子内的局部变量代替Accumulators，没有flink提供的容错恢复
 *
 * Accumulators目前只能在任务结束时获取，意味着Accumulators适用与dataset统计多一点。
 * 使用方式例如：
 * ①创建Accumulators实例
 * private IntCounter numLines = new IntCounter();
 * ②在flink任务环境中注册Accumulators实列
 * getRuntimeContext().addAccumulator("num-lines", this.numLines);
 * ③在flink任意算子中使用Accumulators
 * this.numLines.add(1);
 * ④在flink任务结束之后获取Accumulators的值
 * myJobExecutionResult.getAccumulatorResult("num-lines")
 */
public class $01Accumulators {
    public static void main(String[] args) throws Exception {


        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // job执行环境配置
        StreamConfig.setCheckPoint(env);
        env.setParallelism(100);


        SingleOutputStreamOperator<Long> mapStream = env.fromSequence(1, Integer.MAX_VALUE)
//        SingleOutputStreamOperator<Long> mapStream = env.fromSequence(1, 20)
                .map(new RichMapFunction<Long, Long>() {

                    private Accumulator<Long, Long> counter = new LongCounter();

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        getRuntimeContext().addAccumulator("counter", this.counter);
                    }

                    @Override
                    public Long map(Long value) throws Exception {
                        if (value % 3 == 0) {
                            this.counter.add(1L);
                        }
                        return 0L;
                    }
                });

        JobExecutionResult executionResult = env.execute();
        Long counter = executionResult.getAccumulatorResult("counter");
        System.out.println(counter);

    }
}
