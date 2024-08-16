package cn.hedeoer.common.datatypes;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class OrderInfo {
    
    private Long id;                // 编号（主键）
    private Long userId;            // 用户id
    private BigDecimal originAmount; // 原始金额
    private BigDecimal couponReduce; // 优惠券减免
    private BigDecimal finalAmount;  // 最终金额
    private String orderStatus;      // 订单状态
    private String outTradeNo;       // 订单交易编号（第三方支付用）
    private String tradeBody;        // 订单描述（第三方支付用）
    private String sessionId;        // 会话id
    private Integer provinceId;      // 省份id
    private String createTime; // 创建时间
    private String expireTime; // 失效时间
    private String updateTime; // 更新时间

    // Getters and Setters
    
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public BigDecimal getOriginAmount() {
        return originAmount;
    }

    public void setOriginAmount(BigDecimal originAmount) {
        this.originAmount = originAmount;
    }

    public BigDecimal getCouponReduce() {
        return couponReduce;
    }

    public void setCouponReduce(BigDecimal couponReduce) {
        this.couponReduce = couponReduce;
    }

    public BigDecimal getFinalAmount() {
        return finalAmount;
    }

    public void setFinalAmount(BigDecimal finalAmount) {
        this.finalAmount = finalAmount;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getOutTradeNo() {
        return outTradeNo;
    }

    public void setOutTradeNo(String outTradeNo) {
        this.outTradeNo = outTradeNo;
    }

    public String getTradeBody() {
        return tradeBody;
    }

    public void setTradeBody(String tradeBody) {
        this.tradeBody = tradeBody;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Integer getProvinceId() {
        return provinceId;
    }

    public void setProvinceId(Integer provinceId) {
        this.provinceId = provinceId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(String expireTime) {
        this.expireTime = expireTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
