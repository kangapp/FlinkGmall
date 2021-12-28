package com.flink.bean;

import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;

public class PaymentWide {
    Long payment_id;
    String subject;
    String payment_type;
    String payment_create_time;
    String callback_time;
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
    String order_create_time;
    String province_name; //查询维表得到
    String province_area_code;
    String province_iso_code;
    String province_3166_2_code;
    Integer user_age; //用户信息
    String user_gender;
    Long spu_id; //作为维度数据 要关联进来
    Long tm_id;
    Long category3_id;
    String spu_name;
    String tm_name;
    String category3_name;

    public PaymentWide() {
    }

    public PaymentWide(Long payment_id, String subject, String payment_type, String payment_create_time, String callback_time, Long detail_id, Long order_id, Long sku_id, BigDecimal order_price, Long sku_num, String sku_name, Long province_id, String order_status, Long user_id, BigDecimal total_amount, BigDecimal activity_reduce_amount, BigDecimal coupon_reduce_amount, BigDecimal original_total_amount, BigDecimal feight_fee, BigDecimal split_feight_fee, BigDecimal split_activity_amount, BigDecimal split_coupon_amount, BigDecimal split_total_amount, String order_create_time, String province_name, String province_area_code, String province_iso_code, String province_3166_2_code, Integer user_age, String user_gender, Long spu_id, Long tm_id, Long category3_id, String spu_name, String tm_name, String category3_name) {
        this.payment_id = payment_id;
        this.subject = subject;
        this.payment_type = payment_type;
        this.payment_create_time = payment_create_time;
        this.callback_time = callback_time;
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
        this.order_create_time = order_create_time;
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

    public Long getPayment_id() {
        return payment_id;
    }

    public void setPayment_id(Long payment_id) {
        this.payment_id = payment_id;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getPayment_type() {
        return payment_type;
    }

    public void setPayment_type(String payment_type) {
        this.payment_type = payment_type;
    }

    public String getPayment_create_time() {
        return payment_create_time;
    }

    public void setPayment_create_time(String payment_create_time) {
        this.payment_create_time = payment_create_time;
    }

    public String getCallback_time() {
        return callback_time;
    }

    public void setCallback_time(String callback_time) {
        this.callback_time = callback_time;
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

    public String getOrder_create_time() {
        return order_create_time;
    }

    public void setOrder_create_time(String order_create_time) {
        this.order_create_time = order_create_time;
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

    public PaymentWide(PaymentInfo paymentInfo, OrderWide orderWide) {
        mergeOrderWide(orderWide);
        mergePaymentInfo(paymentInfo);
    }
    public void mergePaymentInfo(PaymentInfo paymentInfo) {
        if (paymentInfo != null) {
            try {
                BeanUtils.copyProperties(this, paymentInfo);
                payment_create_time = paymentInfo.create_time;
                payment_id = paymentInfo.id;
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }
    public void mergeOrderWide(OrderWide orderWide) {
        if (orderWide != null) {
            try {
                BeanUtils.copyProperties(this, orderWide);
                order_create_time = orderWide.create_time;
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return "PaymentWide{" +
                "payment_id=" + payment_id +
                ", subject='" + subject + '\'' +
                ", payment_type='" + payment_type + '\'' +
                ", payment_create_time='" + payment_create_time + '\'' +
                ", callback_time='" + callback_time + '\'' +
                ", detail_id=" + detail_id +
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
                ", order_create_time='" + order_create_time + '\'' +
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
