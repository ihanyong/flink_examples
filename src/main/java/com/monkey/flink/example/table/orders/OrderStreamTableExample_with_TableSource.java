package com.monkey.flink.example.table.orders;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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
import java.util.TimeZone;

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
public class OrderStreamTableExample_with_TableSource {

    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


        DataStream<Order> orderStream = env.addSource(new OrderStreamSource()).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(3)) {

            @Override
            public long extractTimestamp(Order element) {
                return element.getOrderTime().getTime();
            }
        });

//        tableEnv.registerTableSource("orders", new OrderTableSourceWithRowtimeAttributes());
//        tableEnv.registerTableSource("orders", new OrderTableSourceWithoutRowtimeAttributes());
        tableEnv.registerDataStream("orders", orderStream, "orderId,comment,owner,shop,amount, orderTime,t.rowtime ");


        Table ownerOrderCount = tableEnv
                .sqlQuery("select owner,  TIMESTAMPADD(HOUR , 8,TUMBLE_START(t , INTERVAL '3' SECOND)) , count(orderId)  from orders group by owner, TUMBLE(t, INTERVAL '3' SECOND)");
//                .sqlQuery("select owner,  t , orderId from orders ");
//                .scan("orders")
//                .window(Tumble.over("3.second").on("t").as("w"))
//                .groupBy("w, owner")
//                .select("owner, w.end , orderId.count as count");


//        DataStream<Tuple2<Boolean, Tuple4<String, Timestamp,Timestamp, Long>>> countStream = tableEnv.toRetractStream(ownerOrderCount, TypeInformation.of(new TypeHint<Tuple4<String,Timestamp,Timestamp, Long>>() {}));
        DataStream<Tuple2<Boolean, Tuple3<String, Timestamp, Long>>> countStream = tableEnv.toRetractStream(ownerOrderCount, TypeInformation.of(new TypeHint<Tuple3<String,Timestamp, Long>>() {}));


        countStream.print();


        env.execute("order table");


    }
}
