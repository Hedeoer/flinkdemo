package cn.hedeoer.common.datatypes;

// 商品收藏事件
public class Favorite {
    public String userId;
    public String productId;
    public Long favoriteTime; // 事件时间戳, 毫秒

    // Flink需要无参构造
    public Favorite() {}

    public Favorite(String userId, String productId, Long favoriteTime) {
        this.userId = userId;
        this.productId = productId;
        this.favoriteTime = favoriteTime;
    }

    @Override
    public String toString() {
        return "Favorite{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", favoriteTime=" + favoriteTime +
                '}';
    }
}