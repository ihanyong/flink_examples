package com.monkey.flink.example.table.orders;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Order
 *
 * @author yong.han
 * 2019/3/7
 */
public class Order implements Serializable, Cloneable {
    private long orderId;
    private String comment;
    private String owner;
    private String shop;
    private double amount;
    private long orderTime;


    public Order() { }
    public Order(long orderId, String comment, String owner, String shop, double amount, Long orderTime) {
        this.orderId = orderId;
        this.comment = comment;
        this.owner = owner;
        this.shop = shop;
        this.amount = amount;
        this.orderTime = null == orderTime ? 0 : orderTime;
    }


    private Timestamp t;

    public Timestamp getT() {
        return t;
    }

    public void setT(Timestamp t) {
        this.t = t;
    }

    @Override
    protected Order clone()  {
        return new Order(this.orderId, this.comment, this.owner, this.shop, this.amount, this.orderTime);
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getShop() {
        return shop;
    }

    public void setShop(String shop) {
        this.shop = shop;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public long getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(long orderTime) {
        this.orderTime = orderTime;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId=" + orderId +
                ", comment='" + comment + '\'' +
                ", owner='" + owner + '\'' +
                ", shop='" + shop + '\'' +
                ", amount=" + amount +
                ", orderTime=" + orderTime +
                '}';
    }

    private static final long serialVersionUID = -1L;
}
