package com.monkey.flink.example.table.orders;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;

import java.sql.Timestamp;

/**
 * OrderStreamTableExample
 *
 *
 *
 * 1. org.apache.flink.table.api.TableException: Type is not supported: Date
 *
 *
 *
 *
 *
 * @author yong.han
 * 2019/3/7
 */
public class OrderStreamTableExample_with_StreamConvert2Table {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
//        tableEnv.registerTableSource("orders", new OrderTableSourceWithRowtimeAttributes());

//        private long orderId;
//        private String comment;
//        private String owner;
//        private String shop;
//        private double amount;
//        private Timestamp orderTime;
        DataStream<Order> source = env.addSource(new OrderStreamSource());


//        Table ownerOrderCount = tableEnv.scan("orders")
        DataStream<Order>  timerDataStream = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.milliseconds(100)) {
            @Override
            public long extractTimestamp(Order element) {
                return element.getOrderTime().getTime();
            }
        });

        tableEnv.registerDataStream("orders", timerDataStream, "orderId,owner, orderTime.rowtime ");
        Table ordersTable = tableEnv.scan("orders");

//        Table ordersTable = tableEnv.fromDataStream( timerDataStream, "orderId,owner, orderTime.rowtime ");

        Table ownerOrderCount = ordersTable
                .window(Tumble.over("3.second").on("orderTime").as("w"))
                .groupBy("w, owner")
                .select("owner, w.end , orderId.count as count");


        DataStream<Tuple2<Boolean, Tuple3<String, Timestamp, Long>>> countStream = tableEnv.toRetractStream(ownerOrderCount, TypeInformation.of(new TypeHint<Tuple3<String,Timestamp, Long>>() {}));

        countStream.print();


        env.execute("order table");

//        System.out.println(env.getExecutionPlan());

    }
}
