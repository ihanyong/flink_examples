package com.monkey.flink.example.table.orders.join;

import com.monkey.flink.example.table.orders.OrderTableSourceWithRowtimeAttributes;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

/**
 * TableJoinTemporal
 *
 * @author yong.han
 * 2019/3/19
 */
public class TableJoinTemporal {
    public static final long startTime = System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);


        tEnv.registerTableSource("order", new OrderTableSourceWithRowtimeAttributes());

//        DataStream<User> userDataStream = env.addSource(new UserStreamSource());
//        tEnv.registerDataStream("user_update", userDataStream, "id, name, updateTime, updateTimestamp.rowtime");
        tEnv.registerTableSource("user_update", new UserStreamTableSource());

        Table orderTable = tEnv.scan("order").select("orderId, owner, shop, amount,t, orderTime");
        Table userUpdateTable = tEnv.scan("user_update").select("id, name,updateTime as userUpdateTime, updateTimestamp");
        TemporalTableFunction tableFunction = userUpdateTable.createTemporalTableFunction("updateTimestamp", "id");
        tEnv.registerFunction("user", tableFunction);



        Table joinedTable = orderTable.join(new Table(tEnv,"user(t)"), "owner = id")
                .select("orderId, owner , name as ownerName, shop, amount, orderTime,userUpdateTime ");



        tEnv.toRetractStream(joinedTable, TypeInformation.of(TableWindowJoin.Pojo.class))
                .addSink(new SinkFunction<Tuple2<Boolean, TableWindowJoin.Pojo>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, TableWindowJoin.Pojo> value, Context context) throws Exception {
                        System.out.println(String.format("%3$2s : %1$2s : %2$2s", value.f0, value.f1, System.currentTimeMillis() - startTime));
                    }
                });

        env.execute("Join table");

    }
}
