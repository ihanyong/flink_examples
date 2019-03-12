package com.monkey.flink.example.table.orders;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class OrderStreamTableExample_with_SQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        tableEnv.registerTableSource("orders", new OrderTableSourceWithRowtimeAttributes());


//        Table ownerOrderCount = tableEnv.scan("orders")
//                .window(Tumble.over("3.second").on("orderTime").as("w"))
//                .groupBy("w, owner")
//                .select("owner, w.end , orderId.count as count");

        Table ownerOrderCount = tableEnv.sqlQuery(
                "select owner, TUMBLE_END(orderTime, INTERVAL '3' second), count(orderId)" +
                " from orders " +
                "group by owner, TUMBLE(orderTime, INTERVAL '3' second)");


        DataStream<Tuple2<Boolean, Tuple3<String, Timestamp, Long>>> countStream = tableEnv.toRetractStream(ownerOrderCount, TypeInformation.of(new TypeHint<Tuple3<String,Timestamp, Long>>() {}));

        countStream.print();


        env.execute("order table");


    }
}
