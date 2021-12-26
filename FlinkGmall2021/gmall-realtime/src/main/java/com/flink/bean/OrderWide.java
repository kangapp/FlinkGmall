package com.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;

import java.math.BigDecimal;

/**
 * 订单表 + 订单明细表 + 所需的维度表
 */
public class OrderWide {
    Long detail_id;
    Long order_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    Long province_id;
    String order_status;
    Long user_id;

    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    BigDecimal split_feight_fee;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    BigDecimal split_total_amount;

    String expire_time;
    String create_time; //yyyy-MM-dd HH:mm:ss
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;

    String province_name;//查询维表得到
    String province_area_code;
    String province_iso_code;
    String province_3166_2_code;

    Integer user_age;
    String user_gender;

    Long spu_id; //作为维度数据 要关联进来
    Long tm_id;
    Long category3_id;
    String spu_name;
    String tm_name;
    String category3_name;

    public OrderWide() {
    }

    public OrderWide(Long detail_id, Long order_id, Long sku_id, BigDecimal order_price, Long sku_num, String sku_name, Long province_id, String order_status, Long user_id, BigDecimal total_amount, BigDecimal activity_reduce_amount, BigDecimal coupon_reduce_amount, BigDecimal original_total_amount, BigDecimal feight_fee, BigDecimal split_feight_fee, BigDecimal split_activity_amount, BigDecimal split_coupon_amount, BigDecimal split_total_amount, String expire_time, String create_time, String operate_time, String create_date, String create_hour, String province_name, String province_area_code, String province_iso_code, String province_3166_2_code, Integer user_age, String user_gender, Long spu_id, Long tm_id, Long category3_id, String spu_name, String tm_name, String category3_name) {
        this.detail_id = detail_id;
        this.order_id = order_id;
        this.sku_id = sku_id;
        this.order_price = order_price;
        this.sku_num = sku_num;
        this.sku_name = sku_name;
        this.province_id = province_id;
        this.order_status = order_status;
        this.user_id = user_id;
        this.total_amount = total_amount;
        this.activity_reduce_amount = activity_reduce_amount;
        this.coupon_reduce_amount = coupon_reduce_amount;
        this.original_total_amount = original_total_amount;
        this.feight_fee = feight_fee;
        this.split_feight_fee = split_feight_fee;
        this.split_activity_amount = split_activity_amount;
        this.split_coupon_amount = split_coupon_amount;
        this.split_total_amount = split_total_amount;
        this.expire_time = expire_time;
        this.create_time = create_time;
        this.operate_time = operate_time;
        this.create_date = create_date;
        this.create_hour = create_hour;
        this.province_name = province_name;
        this.province_area_code = province_area_code;
        this.province_iso_code = province_iso_code;
        this.province_3166_2_code = province_3166_2_code;
        this.user_age = user_age;
        this.user_gender = user_gender;
        this.spu_id = spu_id;
        this.tm_id = tm_id;
        this.category3_id = category3_id;
        this.spu_name = spu_name;
        this.tm_name = tm_name;
        this.category3_name = category3_name;
    }

    public Long getDetail_id() {
        return detail_id;
    }

    public void setDetail_id(Long detail_id) {
        this.detail_id = detail_id;
    }

    public Long getOrder_id() {
        return order_id;
    }

    public void setOrder_id(Long order_id) {
        this.order_id = order_id;
    }

    public Long getSku_id() {
        return sku_id;
    }

    public void setSku_id(Long sku_id) {
        this.sku_id = sku_id;
    }

    public BigDecimal getOrder_price() {
        return order_price;
    }

    public void setOrder_price(BigDecimal order_price) {
        this.order_price = order_price;
    }

    public Long getSku_num() {
        return sku_num;
    }

    public void setSku_num(Long sku_num) {
        this.sku_num = sku_num;
    }

    public String getSku_name() {
        return sku_name;
    }

    public void setSku_name(String sku_name) {
        this.sku_name = sku_name;
    }

    public Long getProvince_id() {
        return province_id;
    }

    public void setProvince_id(Long province_id) {
        this.province_id = province_id;
    }

    public String getOrder_status() {
        return order_status;
    }

    public void setOrder_status(String order_status) {
        this.order_status = order_status;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public BigDecimal getTotal_amount() {
        return total_amount;
    }

    public void setTotal_amount(BigDecimal total_amount) {
        this.total_amount = total_amount;
    }

    public BigDecimal getActivity_reduce_amount() {
        return activity_reduce_amount;
    }

    public void setActivity_reduce_amount(BigDecimal activity_reduce_amount) {
        this.activity_reduce_amount = activity_reduce_amount;
    }

    public BigDecimal getCoupon_reduce_amount() {
        return coupon_reduce_amount;
    }

    public void setCoupon_reduce_amount(BigDecimal coupon_reduce_amount) {
        this.coupon_reduce_amount = coupon_reduce_amount;
    }

    public BigDecimal getOriginal_total_amount() {
        return original_total_amount;
    }

    public void setOriginal_total_amount(BigDecimal original_total_amount) {
        this.original_total_amount = original_total_amount;
    }

    public BigDecimal getFeight_fee() {
        return feight_fee;
    }

    public void setFeight_fee(BigDecimal feight_fee) {
        this.feight_fee = feight_fee;
    }

    public BigDecimal getSplit_feight_fee() {
        return split_feight_fee;
    }

    public void setSplit_feight_fee(BigDecimal split_feight_fee) {
        this.split_feight_fee = split_feight_fee;
    }

    public BigDecimal getSplit_activity_amount() {
        return split_activity_amount;
    }

    public void setSplit_activity_amount(BigDecimal split_activity_amount) {
        this.split_activity_amount = split_activity_amount;
    }

    public BigDecimal getSplit_coupon_amount() {
        return split_coupon_amount;
    }

    public void setSplit_coupon_amount(BigDecimal split_coupon_amount) {
        this.split_coupon_amount = split_coupon_amount;
    }

    public BigDecimal getSplit_total_amount() {
        return split_total_amount;
    }

    public void setSplit_total_amount(BigDecimal split_total_amount) {
        this.split_total_amount = split_total_amount;
    }

    public String getExpire_time() {
        return expire_time;
    }

    public void setExpire_time(String expire_time) {
        this.expire_time = expire_time;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getOperate_time() {
        return operate_time;
    }

    public void setOperate_time(String operate_time) {
        this.operate_time = operate_time;
    }

    public String getCreate_date() {
        return create_date;
    }

    public void setCreate_date(String create_date) {
        this.create_date = create_date;
    }

    public String getCreate_hour() {
        return create_hour;
    }

    public void setCreate_hour(String create_hour) {
        this.create_hour = create_hour;
    }

    public String getProvince_name() {
        return province_name;
    }

    public void setProvince_name(String province_name) {
        this.province_name = province_name;
    }

    public String getProvince_area_code() {
        return province_area_code;
    }

    public void setProvince_area_code(String province_area_code) {
        this.province_area_code = province_area_code;
    }

    public String getProvince_iso_code() {
        return province_iso_code;
    }

    public void setProvince_iso_code(String province_iso_code) {
        this.province_iso_code = province_iso_code;
    }

    public String getProvince_3166_2_code() {
        return province_3166_2_code;
    }

    public void setProvince_3166_2_code(String province_3166_2_code) {
        this.province_3166_2_code = province_3166_2_code;
    }

    public Integer getUser_age() {
        return user_age;
    }

    public void setUser_age(Integer user_age) {
        this.user_age = user_age;
    }

    public String getUser_gender() {
        return user_gender;
    }

    public void setUser_gender(String user_gender) {
        this.user_gender = user_gender;
    }

    public Long getSpu_id() {
        return spu_id;
    }

    public void setSpu_id(Long spu_id) {
        this.spu_id = spu_id;
    }

    public Long getTm_id() {
        return tm_id;
    }

    public void setTm_id(Long tm_id) {
        this.tm_id = tm_id;
    }

    public Long getCategory3_id() {
        return category3_id;
    }

    public void setCategory3_id(Long category3_id) {
        this.category3_id = category3_id;
    }

    public String getSpu_name() {
        return spu_name;
    }

    public void setSpu_name(String spu_name) {
        this.spu_name = spu_name;
    }

    public String getTm_name() {
        return tm_name;
    }

    public void setTm_name(String tm_name) {
        this.tm_name = tm_name;
    }

    public String getCategory3_name() {
        return category3_name;
    }

    public void setCategory3_name(String category3_name) {
        this.category3_name = category3_name;
    }

    public OrderWide(OrderInfo orderInfo, OrderDetail orderDetail) {
        mergeOrderInfo(orderInfo);
        mergeOrderDetail(orderDetail);
    }

    public void mergeOrderInfo(OrderInfo orderInfo) {
        if (orderInfo != null) {
            this.order_id = orderInfo.id;
            this.order_status = orderInfo.order_status;
            this.create_time = orderInfo.create_time;
            this.create_date = orderInfo.create_date;
            this.create_hour = orderInfo.create_hour;
            this.activity_reduce_amount = orderInfo.activity_reduce_amount;
            this.coupon_reduce_amount = orderInfo.coupon_reduce_amount;
            this.original_total_amount = orderInfo.original_total_amount;
            this.feight_fee = orderInfo.feight_fee;
            this.total_amount = orderInfo.total_amount;
            this.province_id = orderInfo.province_id;
            this.user_id = orderInfo.user_id;
        }
    }

    public void mergeOrderDetail(OrderDetail orderDetail) {
        if (orderDetail != null) {
            this.detail_id = orderDetail.id;
            this.sku_id = orderDetail.sku_id;
            this.sku_name = orderDetail.sku_name;
            this.order_price = orderDetail.order_price;
            this.sku_num = orderDetail.sku_num;
            this.split_activity_amount = orderDetail.split_activity_amount;
            this.split_coupon_amount = orderDetail.split_coupon_amount;
            this.split_total_amount = orderDetail.split_total_amount;
        }
    }

    public void mergeOtherOrderWide(OrderWide otherOrderWide) {
        this.order_status = ObjectUtils.firstNonNull(this.order_status,
                otherOrderWide.order_status);
        this.create_time = ObjectUtils.firstNonNull(this.create_time,
                otherOrderWide.create_time);
        this.create_date = ObjectUtils.firstNonNull(this.create_date,
                otherOrderWide.create_date);
        this.coupon_reduce_amount = ObjectUtils.firstNonNull(this.coupon_reduce_amount,
                otherOrderWide.coupon_reduce_amount);
        this.activity_reduce_amount = ObjectUtils.firstNonNull(this.activity_reduce_amount,
                otherOrderWide.activity_reduce_amount);
        this.original_total_amount = ObjectUtils.firstNonNull(this.original_total_amount,
                otherOrderWide.original_total_amount);
        this.feight_fee = ObjectUtils.firstNonNull(this.feight_fee, otherOrderWide.feight_fee);
        this.total_amount = ObjectUtils.firstNonNull(this.total_amount,
                otherOrderWide.total_amount);
        this.user_id = ObjectUtils.<Long>firstNonNull(this.user_id, otherOrderWide.user_id);
        this.sku_id = ObjectUtils.firstNonNull(this.sku_id, otherOrderWide.sku_id);
        this.sku_name = ObjectUtils.firstNonNull(this.sku_name, otherOrderWide.sku_name);
        this.order_price = ObjectUtils.firstNonNull(this.order_price,
                otherOrderWide.order_price);
        this.sku_num = ObjectUtils.firstNonNull(this.sku_num, otherOrderWide.sku_num);
        this.split_activity_amount = ObjectUtils.firstNonNull(this.split_activity_amount);
        this.split_coupon_amount = ObjectUtils.firstNonNull(this.split_coupon_amount);
        this.split_total_amount = ObjectUtils.firstNonNull(this.split_total_amount);
    }

    @Override
    public String toString() {
        return "OrderWide{" +
                "detail_id=" + detail_id +
                ", order_id=" + order_id +
                ", sku_id=" + sku_id +
                ", order_price=" + order_price +
                ", sku_num=" + sku_num +
                ", sku_name='" + sku_name + '\'' +
                ", province_id=" + province_id +
                ", order_status='" + order_status + '\'' +
                ", user_id=" + user_id +
                ", total_amount=" + total_amount +
                ", activity_reduce_amount=" + activity_reduce_amount +
                ", coupon_reduce_amount=" + coupon_reduce_amount +
                ", original_total_amount=" + original_total_amount +
                ", feight_fee=" + feight_fee +
                ", split_feight_fee=" + split_feight_fee +
                ", split_activity_amount=" + split_activity_amount +
                ", split_coupon_amount=" + split_coupon_amount +
                ", split_total_amount=" + split_total_amount +
                ", expire_time='" + expire_time + '\'' +
                ", create_time='" + create_time + '\'' +
                ", operate_time='" + operate_time + '\'' +
                ", create_date='" + create_date + '\'' +
                ", create_hour='" + create_hour + '\'' +
                ", province_name='" + province_name + '\'' +
                ", province_area_code='" + province_area_code + '\'' +
                ", province_iso_code='" + province_iso_code + '\'' +
                ", province_3166_2_code='" + province_3166_2_code + '\'' +
                ", user_age=" + user_age +
                ", user_gender='" + user_gender + '\'' +
                ", spu_id=" + spu_id +
                ", tm_id=" + tm_id +
                ", category3_id=" + category3_id +
                ", spu_name='" + spu_name + '\'' +
                ", tm_name='" + tm_name + '\'' +
                ", category3_name='" + category3_name + '\'' +
                '}';
    }
}
