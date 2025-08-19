package cn.hedeoer.common.sources;

import cn.hedeoer.common.datatypes.Heartbeat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.planner.expressions.In;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 一个自定义的 Flink Source，用于模拟生成主机心跳数据。
 * 这个类重新实现了 DataGen 连接器的核心功能，使其成为一个纯粹的 DataStream API 组件。
 */
public class HeartbeatSource implements SourceFunction<Heartbeat> {

    // 控制数据源是否持续运行的标志
    private volatile boolean isRunning = true;

    // 用于生成随机数据的 Random 实例
    private final Random random = new Random();

    // === 可配置的模拟参数 ===
    private final long rowsPerSecond;
    private final long minHostId;
    private final long maxHostId;
    private final double minCpuUsage;
    private final double maxCpuUsage;
    private final double minMemoryUsage;
    private final double maxMemoryUsage;

    /**
     * 构造函数，允许配置所有模拟参数。
     * @param rowsPerSecond 每秒生成的记录数
     * @param minHostId     最小主机ID
     * @param maxHostId     最大主机ID
     * @param minCpuUsage   最小CPU使用率
     * @param maxCpuUsage   最大CPU使用率
     * @param minMemoryUsage 最小内存使用率
     * @param maxMemoryUsage 最大内存使用率
     */
    public HeartbeatSource(long rowsPerSecond, long minHostId, long maxHostId,
                           double minCpuUsage, double maxCpuUsage,
                           double minMemoryUsage, double maxMemoryUsage) {
        this.rowsPerSecond = rowsPerSecond;
        this.minHostId = minHostId;
        this.maxHostId = maxHostId;
        this.minCpuUsage = minCpuUsage;
        this.maxCpuUsage = maxCpuUsage;
        this.minMemoryUsage = minMemoryUsage;
        this.maxMemoryUsage = maxMemoryUsage;
    }

    /**
     * Flink 框架调用的主方法，用于启动数据生成。
     * @param ctx SourceContext 用于向下游发送数据
     * @throws Exception 异常
     */
    @Override
    public void run(SourceContext<Heartbeat> ctx) throws Exception {
        // 计算每条记录之间的休眠时间（毫秒）
        final long sleepTime = 1000 / rowsPerSecond;
        // 当达到限制，限制hostId最小机器的生成模拟数据，模拟中断的场景
        Integer limitTimes = 2;
        //
        HashMap<Long, Long> currentHostIdCount = new HashMap<>();

        while (isRunning) {
            // 1. 生成随机的主机ID
            long randomHostId = minHostId + random.nextInt((int) (maxHostId - minHostId + 1));

            // 2. 生成随机的 CPU 使用率
            double randomCpuUsage = minCpuUsage + random.nextDouble() * (maxCpuUsage - minCpuUsage);

            // 3. 生成随机的内存使用率
            double randomMemoryUsage = minMemoryUsage + random.nextDouble() * (maxMemoryUsage - minMemoryUsage);

            // 4. 创建 Heartbeat 对象
            Heartbeat heartbeat = new Heartbeat(
                    randomHostId,
                    LocalDateTime.now(), // 使用当前时间
                    randomCpuUsage,
                    randomMemoryUsage
            );
            currentHostIdCount.put(randomHostId, currentHostIdCount.getOrDefault(randomHostId, 0L) + 1);
            // 对minHostId的机器模拟中断的场景
            if (currentHostIdCount.get(minHostId) != null && currentHostIdCount.get(minHostId) > limitTimes) {
                heartbeat.hostId = maxHostId;
            }

            // 5. 使用 SourceContext 发送数据
            ctx.collect(heartbeat);

            // 6. 休眠以控制生成速率
            if (sleepTime > 0) {
                TimeUnit.MILLISECONDS.sleep(sleepTime);
            }
        }
    }

    /**
     * Flink 框架调用此方法来取消数据源的运行（例如，当作业停止时）。
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}