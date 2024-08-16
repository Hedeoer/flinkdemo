package cn.hedeoer.chapter02.finkjobverify;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TestStatelessTimelessUDFTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        TestStatelessTimelessUDF.IncrementMapFunction incrementer = new TestStatelessTimelessUDF.IncrementMapFunction();

        // call the methods that you have implemented
        assertEquals(3L, incrementer.map(2L));
    }



}