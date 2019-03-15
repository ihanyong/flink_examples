package com.monkey.flink.example.table.orders;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * OrderStreamTable_Windows_example
 *
 * @author yong.han
 * 2019/3/15
 */
public class OrderStreamTable_Windows_example {

    public static void main(String[] args) {
        invokeTable((evn, stream)-> {
//            Table table = evn.fromDataStream(stream, "owner, amount, eventTime.rowtime");
            Table table = evn.fromDataStream(stream, "owner, amount");
            evn.registerTableSink("outTable", null);

            table = table
                    .groupBy("owner")
                    .select("owner, amount.sum");



            evn.toRetractStream(table, TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
            })).print();




            return table;
        });





    }



    public static void invokeTable(BiFunction<StreamTableEnvironment, DataStream<Order>, Table> function)  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
        DataStream<Order> orderDataStream = env.addSource(new OrderStreamSource()).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.milliseconds(100)) {
            @Override
            public long extractTimestamp(Order element) {
                return element.getOrderTime();
            }
        });

        Table table = function.apply(tableEnv,orderDataStream);


        try {
            env.execute("OrderStreamTable_Windows_example");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
