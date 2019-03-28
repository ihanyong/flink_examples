package com.monkey.flink.example.table.orders.tableapi;

import com.monkey.flink.example.table.orders.Order;
import com.monkey.flink.example.table.orders.OrderTableSourceWithRowtimeAttributes;
import lombok.Data;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.Serializable;

/**
 * TableSPF
 *
 * scan, select, as, filter/where
 *
 *
 * @author yong.han
 * 2019/3/27
 */
public class TableSPF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);


        tableEnv.registerTableSource("orders", new OrderTableSourceWithRowtimeAttributes());

        Table orders = tableEnv.scan("orders")
//                .select("orderId, owner, shop, amount, orderTime")
//                .as("orderId, ownerId, shopId, amount, orderTime");
                .select("orderId, owner as ownerId, shop as shopId, amount, orderTime")
                .where("orderId%3==0 && ownerId == 'owner_1'");


        DataStream dataStream = tableEnv.toRetractStream(orders, FilterOrder.class);

        dataStream.print();

        env.execute("TableSPF");
    }


    @Data
    public static class FilterOrder implements Serializable {
        private long orderId;
//        private String comment;
        private String ownerId;
        private String shopId;
        private double amount;
        private long orderTime;
    }

}
