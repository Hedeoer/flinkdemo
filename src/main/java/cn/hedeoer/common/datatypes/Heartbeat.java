package cn.hedeoer.common.datatypes;

import java.time.LocalDateTime;

public class Heartbeat {

    // 字段已修改为驼峰命名法 (camelCase)
    public Long hostId;
    public LocalDateTime reportTime;
    public Double cpuUsage;
    public Double memoryUsage;

    // Flink POJO 要求必须有一个公共的无参构造函数
    public Heartbeat() {}

    public Heartbeat(Long hostId, LocalDateTime reportTime, Double cpuUsage, Double memoryUsage) {
        this.hostId = hostId;
        this.reportTime = reportTime;
        this.cpuUsage = cpuUsage;
        this.memoryUsage = memoryUsage;
    }

    @Override
    public String toString() {
        // toString 方法也同步更新字段名
        return "Heartbeat{" +
                "hostId=" + hostId +
                ", reportTime=" + reportTime +
                ", cpuUsage=" + cpuUsage +
                ", memoryUsage=" + memoryUsage +
                '}';
    }
}