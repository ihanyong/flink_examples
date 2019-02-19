package com.monkey.flink.example.windowing;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;

/**
 * WindowOnWindowExample
 *
 * 结果：
 * - 时间窗口的划分方式， 不是以第一个到达的时间为起点进行划分， 而是是自然的时间的0时0分0秒为起点进行划分
 * - windowedStream 应用算子(只有聚合类)后的结果是一个 datastream
 * - 在结果datastream 上再 进行window 划分，和普通的 window 操作没有区别， 只需要注意 event time 的问题
 * - TimestampAssigner 没有再指定的话， 还是沿用上一个， 指定新的会在结果data stream  上应用新的TimestampAssigner
 * @author yong.han
 * 2019/2/19
 */
public class WindowOnWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<String, Long, Integer>> source = env.fromCollection(Arrays.asList(
                Tuple3.of("key_", 10_000L, 1)
                ,Tuple3.of("key_", 11_000L, 1)
                ,Tuple3.of("key_", 12_000L, 1)
                ,Tuple3.of("key_", 13_000L, 1)
                ,Tuple3.of("key_", 13_030L, 1)
                ,Tuple3.of("key_", 14_000L, 1)
                ,Tuple3.of("key_", 15_000L, 1)
                ,Tuple3.of("key_", 16_000L, 1)
                ,Tuple3.of("key_", 17_000L, 1)
                ,Tuple3.of("key_", 17_020L, 1)
                ,Tuple3.of("key_", 18_000L, 1)
                ,Tuple3.of("key_", 18_040L, 1)
                ,Tuple3.of("key_", 19_000L, 1)
                ,Tuple3.of("key_", 20_000L, 1)
                ,Tuple3.of("key_", 21_000L, 1)
                ,Tuple3.of("key_", 21_090L, 1)
                ,Tuple3.of("key_", 22_000L, 1)
        ));


        source
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<String, Long, Integer> element) {
                        return element.f1;
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5)).sum(2)// (key_,10000,6) (key_,15000,7)(key_,20000,4)
//                .timeWindowAll(Time.seconds(10)).sum(2) // (key_,10000,13) (key_,20000,4)
//                .timeWindowAll(Time.seconds(15)).sum(2) // (key_,10000,6) (key_,15000,11)
//                .timeWindowAll(Time.seconds(15)).sum(2) // (key_,10000,6) (key_,15000,11)


                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>(Time.milliseconds(6)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element) {
                        return element.f2.longValue();
                    }
                }).timeWindowAll(Time.milliseconds(7)).sum(1)
                .print();

        env.execute("WindowOnWindowExample");

    }
}
