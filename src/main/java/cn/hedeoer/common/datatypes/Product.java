package cn.hedeoer.common.datatypes;

// 商品维度信息
public class Product {
    public Long productId;
    public String productName;
    public Double price;

    // Flink需要一个无参构造函数
    public Product() {
    }

    public Product(Long productId, String productName, Double price) {
        this.productId = productId;
        this.productName = productName;
        this.price = price;
    }

    @Override
    public String toString() {
        return "Product{" +
                "productId=" + productId +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                '}';
    }
}