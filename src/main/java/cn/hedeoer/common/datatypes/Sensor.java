package cn.hedeoer.common.datatypes;

public class Sensor {
    private String id;    // 传感器的唯一标识符
    private String type;  // 传感器类型（如温度传感器、湿度传感器等）

    public Sensor() {}

    // 构造方法
    public Sensor(String id, String type) {
        this.id = id;
        this.type = type;
    }

    // Getter 和 Setter 方法
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    // 重写toString方法，便于输出传感器信息
    @Override
    public String toString() {
        return "Sensor{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
