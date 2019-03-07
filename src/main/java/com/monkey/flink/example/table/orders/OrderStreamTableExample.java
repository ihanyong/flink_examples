package com.monkey.flink.example.table.orders;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * OrderStreamTableExample
 *
 * @author yong.han
 * 2019/3/7
 */
public class OrderStreamTableExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


        tableEnv.registerTableSource("orders", new );
    }
}
