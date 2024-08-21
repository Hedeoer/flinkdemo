package cn.hedeoer.common.datatypes;

public class SensorBling {
    public String sensorId; // 感应器ID
    public Long knockTime; // 报警时间，类型为Long

    // 构造函数
    public SensorBling(String sensorId, Long knockTime) {
        this.sensorId = sensorId;
        this.knockTime = knockTime;
    }

    // 感应器ID的getter方法
    public String getSensorId() {
        return sensorId;
    }

    // 感应器ID的setter方法
    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    // 报警时间的getter方法
    public Long getKnockTime() {
        return knockTime;
    }

    // 报警时间的setter方法
    public void setKnockTime(Long knockTime) {
        this.knockTime = knockTime;
    }

    // toString方法，用于打印对象信息
    @Override
    public String toString() {
        return "SensorBling{" +
                "sensorId='" + sensorId + '\'' +
                ", knockTime=" + knockTime +
                '}';
    }
}
