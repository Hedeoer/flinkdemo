package cn.hedeoer.common.datatypes;

// 商品浏览事件
public class ProductView {
    public String userId;
    public String productId;
    public String sessionId;
    public Long viewTime; // 事件时间戳, 毫秒

    // Flink需要无参构造
    public ProductView() {}

    public ProductView(String userId, String productId, String sessionId, Long viewTime) {
        this.userId = userId;
        this.productId = productId;
        this.sessionId = sessionId;
        this.viewTime = viewTime;
    }

    @Override
    public String toString() {
        return "ProductView{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", viewTime=" + viewTime +
                '}';
    }
}