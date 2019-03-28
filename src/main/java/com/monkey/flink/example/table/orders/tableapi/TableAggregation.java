package com.monkey.flink.example.table.orders.tableapi;

import com.monkey.flink.example.table.orders.OrderTableSourceWithRowtimeAttributes;
import lombok.Data;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.Over;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;
import java.util.function.Function;

/**
 * TableAggregation
 *
 * @author yong.han
 * 2019/3/27
 */
public class TableAggregation {


    public static void invoke(Class<?> sinkClass, Function<Table, Table> function) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);


        tableEnv.registerTableSource("orders", new OrderTableSourceWithRowtimeAttributes());

        Table orders = function.apply(tableEnv.scan("orders"));


        DataStream dataStream = tableEnv.toRetractStream(orders, sinkClass);

        dataStream.print();

        env.execute("TableAggregation");
    }


    public static void main(String[] args) throws Exception {
//        invoke(GroupOrder.class,
//                t -> t.select("orderId, owner as ownerId, shop as shopId, amount, orderTime")
//                        .groupBy("ownerId").select("ownerId, amount.sum as amount"));


        invoke(GroupOrder.class,
                t -> t.select("orderId, owner as ownerId, shop as shopId, amount, orderTime")
                        .groupBy("ownerId").select("ownerId, amount.sum.distinct as amount"));


//        invoke(GroupWindowOrder.class,
//                t -> t.window(Tumble.over("5.seconds").on("t").as("w"))
//                        .groupBy("w").select("w.end as windowEnd, amount.sum as total"));

//        invoke(OverWindowOrder.class,
//                t -> t//.select("orderId, owner as ownerId, shop as shopId, amount, orderTime")
//                        .window(Over.orderBy("t").preceding("5.seconds").as("w"))
//                        .select(" orderId, orderTime, amount.sum over w as total, amount.avg over w as avg"));

//        invoke(OverWindowOrder.class,
//                t -> t//.select("orderId, owner as ownerId, shop as shopId, amount, orderTime")
//                        .window(Over.orderBy("t").preceding("5.seconds").as("w"))
//                        .select(" orderId, orderTime, amount.sum over w as total, amount.avg over w as avg"));

    }


    @Data
    public static class OverWindowOrder implements Serializable {
        private long orderTime;
        private long orderId;
        private double total;
        private double avg;
    }


    @Data
    public static class GroupWindowOrder implements Serializable {
//        private long orderId;
//        private String comment;
//        private String ownerId;
//        private String shopId;
        private double total;
        private Timestamp windowEnd;
//        private long orderTime;
    }

    @Data
    public static class GroupOrder implements Serializable {
//        private long orderId;
//        private String comment;
        private String ownerId;
//        private String shopId;
        private double amount;
//        private long orderTime;
    }

}
