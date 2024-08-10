package cn.hedeoer.common.datatypes;

public class WaterSensor {
    private String sensorId;
    private Long ts;
    private Long waterLine;

    public WaterSensor() {
    }

    public WaterSensor(String sensorId, Long ts, Long waterLine) {
        this.sensorId = sensorId;
        this.ts = ts;
        this.waterLine = waterLine;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Long getWaterLine() {
        return waterLine;
    }

    public void setWaterLine(Long waterLine) {
        this.waterLine = waterLine;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "sensorId='" + sensorId + '\'' +
                ", ts=" + ts +
                ", waterLine=" + waterLine +
                '}';
    }
}
