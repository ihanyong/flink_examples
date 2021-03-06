package com.monkey.flink.example.table.orders.join;

import com.monkey.flink.example.table.orders.OrderTableSourceWithRowtimeAttributes;
import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * TableWindowJoin
 *
 * @author yong.han
 * 2019/3/19
 */
public class TableWindowJoin {

    public static final long startTime = System.currentTimeMillis();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);


        tEnv.registerTableSource("order", new OrderTableSourceWithRowtimeAttributes());
        tEnv.registerTableSource("user", new UserStreamTableSource());



        Table orderTable = tEnv.scan("order").select("orderId, owner, shop, amount,orderTime, t");
        Table userTable = tEnv.scan("user").select("id, name,updateTime as userUpdateTime, updateTimestamp");


        Table joinedTable = orderTable.leftOuterJoin(userTable, "owner = id && t >= updateTimestamp && t < updateTimestamp + 10.seconds")
                .select("orderId, owner , name as ownerName, shop, amount, orderTime,userUpdateTime ");

        tEnv.toRetractStream(joinedTable, TypeInformation.of(Pojo.class))
                .addSink(new SinkFunction<Tuple2<Boolean, Pojo>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Pojo> value, Context context) throws Exception {
                        System.out.println(String.format("%3$2s : %1$2s : %2$2s", value.f0, value.f1, System.currentTimeMillis() - startTime));
                    }
                });

        env.execute("Join table");
//        System.out.println(env.getExecutionPlan());


    }


    @Data
    public static class Pojo {
        private long orderId;
        private String owner;
        private String ownerName;
        private String shop;
        private double amount;
        private long orderTime;
        private long userUpdateTime;
    }
}
