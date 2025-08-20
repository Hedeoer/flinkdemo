package cn.hedeoer.common.datatypes;

// 订单流数据
public class Order {
    public Long orderId;
    public Long productId;
    public Integer quantity;

    // Flink需要一个无参构造函数
    public Order() {
    }

    public Order(Long orderId, Long productId, Integer quantity) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId=" + orderId +
                ", productId=" + productId +
                ", quantity=" + quantity +
                '}';
    }
}