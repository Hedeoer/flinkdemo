package cn.hedeoer.chapter01.operations.datastream_transformations;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;

/**
 * richCoMap的使用：
 * 使用CoMap可以共享处理函数中定义的状态，因此适合需要在两个流之间共享信息或状态的场景
 */
public class $RichCoMap {

    /**
     * 交易流中交易金额不小于5000并且用户点击次数大于等于2，则触发告警
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟交易流
        DataStream<Transaction> transactionStream = env.fromElements(
                new Transaction("user_1", 10000, System.currentTimeMillis()), // 这条交易应该触发告警
                new Transaction("user_1", 3000, System.currentTimeMillis()),  // 不应触发告警
                new Transaction("user_1", 3000, System.currentTimeMillis()),  // 不应触发告警
                new Transaction("user_1", 3000, System.currentTimeMillis()),  // 不应触发告警
                new Transaction("user_1", 5000, System.currentTimeMillis()),  // 不应触发告警
                new Transaction("user_1", 5000, System.currentTimeMillis()),  // 不应触发告警
                new Transaction("user_1", 5000, System.currentTimeMillis()),  // 不应触发告警
                new Transaction("user_1", 5000, System.currentTimeMillis()),  // 不应触发告警
                new Transaction("user_1", 5000, System.currentTimeMillis()),  // 不应触发告警
                new Transaction("user_1", 5000, System.currentTimeMillis()),  // 不应触发告警
                new Transaction("user_1", 5000, System.currentTimeMillis()),  // 不应触发告警
                new Transaction("user_1", 5000, System.currentTimeMillis())  // 不应触发告警
        );

        // 模拟用户行为流
        DataStream<UserBehavior> behaviorStream = env.fromElements(
                new UserBehavior("user_1", "click", System.currentTimeMillis()),
                new UserBehavior("user_1", "click", System.currentTimeMillis() + 1000),
                new UserBehavior("user_1", "click", System.currentTimeMillis() + 2000), // 行为计数达到3
                new UserBehavior("user_1", "click", System.currentTimeMillis() + 2000), // 行为计数达到3
                new UserBehavior("user_1", "click", System.currentTimeMillis() + 2000), // 行为计数达到3
                new UserBehavior("user_1", "click", System.currentTimeMillis() + 2000), // 行为计数达到3
                new UserBehavior("user_1", "click", System.currentTimeMillis() + 2000), // 行为计数达到3
                new UserBehavior("user_1", "click", System.currentTimeMillis() + 2000), // 行为计数达到3
                new UserBehavior("user_1", "click", System.currentTimeMillis() + 2000), // 行为计数达到3
                new UserBehavior("user_1", "click", System.currentTimeMillis() + 2000), // 行为计数达到3
                new UserBehavior("user_1", "click", System.currentTimeMillis() + 2000) // 行为计数达到3
        );

        // 连接两个流
        ConnectedStreams<Transaction, UserBehavior> connectedStreams = transactionStream
                .keyBy(transaction -> transaction.getUserId())
                .connect(behaviorStream.keyBy(behavior -> behavior.getUserId()));




        // 使用 CoMapFunction 处理连接流
        DataStream<String> resultStream = connectedStreams.map(new RichCoMapFunction<Transaction, UserBehavior, String>() {

            private  ValueState<Integer> userBehaviorCount;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化共享状态
                ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("userBehaviorCount", Integer.class, 0);
                userBehaviorCount = getRuntimeContext().getState(descriptor);
            }

            @Override
            public String map1(Transaction transaction) throws Exception {
                // 从交易流处理逻辑
                int behaviorCount = userBehaviorCount.value();

                // 检查行为次数和交易金额的条件
                if (transaction.getAmount() >= 5000 && behaviorCount >= 2 ) {
                    return "Warning: Potential fraudulent transaction detected for user: " + transaction.getUserId() +
                            " | Amount: " + transaction.getAmount() +
                            " | User behavior count: " + behaviorCount;
                }
                return "Normal transaction for user: " + transaction.getUserId();
            }

            @Override
            public String map2(UserBehavior behavior) throws Exception {
                // 从用户行为流处理逻辑
                int currentBehaviorCount = userBehaviorCount.value();
                userBehaviorCount.update(currentBehaviorCount + 1);
                return "User: " + behavior.getUserId() + " performed action: " + behavior.getAction() +
                        " | Total behavior count: " + (currentBehaviorCount + 1);
            }
        });

        // 输出结果
        resultStream.print();

        // 执行程序
        env.execute("Real-Time Transaction Monitoring with CoMap");
    }

    // 定义交易信息类
    public static class Transaction {
        private String userId;
        private double amount;
        private long timestamp;

        public Transaction() {}

        public Transaction(String userId, double amount, long timestamp) {
            this.userId = userId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        public String getUserId() {
            return userId;
        }

        public double getAmount() {
            return amount;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    // 定义用户行为类
    public static class UserBehavior {
        private String userId;
        private String action;
        private long timestamp;

        public UserBehavior() {}

        public UserBehavior(String userId, String action, long timestamp) {
            this.userId = userId;
            this.action = action;
            this.timestamp = timestamp;
        }

        public String getUserId() {
            return userId;
        }

        public String getAction() {
            return action;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
