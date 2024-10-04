package cn.hedeoer.chaptor05.TypeFlink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flink 类型核心类
 * org.apache.flink.api.common.typeinfo.TypeInformation，
 * org.apache.flink.api.common.typeinfo.TypeHint
 *
 * 如果在flink中使用了lambda，由于java的类型擦除机制，jvm会在执行擦除泛型类型，flink程序由于未知泛型变量类型而报错。
 * 比如：could not be determined automatically, due to type erasure. You can give type information hints by using the returns(...) method on the result of the transformation call, or by letting your function implement the 'ResultTypeQueryable' interface.
 *
 * 解决方式：
 * 使用return方法，显示的申明返回值的类型
 *
 * retruns方法形参：
 * TypeHint<T> typeHint；
 * TypeInformation<T> typeInfo
 *
 * TypeInformation中的构造函数
 * TypeInformation<T> of(Class<T> typeClass) 使用于pojo类型
 * TypeInformation<T> of(TypeHint<T> typeHint) 使用于基本数据类型
 *
 * TypeHint为抽象类，因此需要实现其中的抽象方法，常常使用匿名内部类的方式
 *
 *
 */
public class LambdaAndFlinkType {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment environmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        environmentWithWebUI.fromElements("11","1")
                .map(x -> Tuple2.of(x,1))
                .returns(new TypeHint<Tuple2<String, Integer>>() {})
//                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))

                .print();

        environmentWithWebUI.execute();
    }
}
