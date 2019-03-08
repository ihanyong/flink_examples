package com.monkey.flink.example.table.clieckevent;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * TableExample
 *
 * @author yong.han
 * 2019/2/13
 */
public class TableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(streamEnv);

        tableEnv.registerTableSource("click_event", new ClickEventTableSource());
        tableEnv.registerTableSink("result", new ClickEventTableSink());


        Table table = tableEnv.scan("click_event");
        Table result = table
                .window(Tumble.over("10.second").on("cTime").as("w"))
                .groupBy("w, user")
                .select("w.end as windowEnd, user, count(user) as clickCount");

        result.insertInto("result");


        streamEnv.execute("stream table example");

//        System.out.println(streamEnv.getExecutionPlan());

    }

}
