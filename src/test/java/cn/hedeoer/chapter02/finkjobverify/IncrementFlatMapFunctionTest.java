package cn.hedeoer.chapter02.finkjobverify;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class IncrementFlatMapFunctionTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        TestStatelessTimelessUDF.IncrementFlatMapFunction incrementer = new TestStatelessTimelessUDF.IncrementFlatMapFunction();

        Collector<Long> collector = mock(Collector.class);

        // call the methods that you have implemented
        incrementer.flatMap(2L, collector);

        //verify collector was called with the right output
        Mockito.verify(collector, times(1)).collect(3L);
    }
}