package com.monkey.flink.example.windownall;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * WindowsAllOnStreamExample
 *
 * 结果： windowAll 是对非keyedStream 进行窗口统计的，
 * 即使是应用在keyedStream 上， 效果也是将keyedStream 降级为 dataSteam 来进行window 划分， 忽略keyed
 *
 * 和keyed on keyedStream 说明了， 定义在dataStream 上的算子就是应用于dataStream的，
 * 不会因为 keyedStream  继承处 dataStream , 在 keyedStream 调用 dataStream 上的算子就有什么区别于 dataStream 的语义
 *
 * @author yong.han
 * 2019/2/19
 */
public class WindowsAllOnStreamExample {
    public static void main(String[] args) throws Exception {
        windowAllOnKeyedStream();
    }


    public static void windowAllOnDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Integer>> source = env.fromCollection(Arrays.asList(
                Tuple3.of("key1_a", "key2_", 1)
                ,Tuple3.of("key1_a", "key2_", 1)
                ,Tuple3.of("key1_a", "key2_", 1)
                ,Tuple3.of("key1_b", "key2_", 1)
                ,Tuple3.of("key1_b", "key2_", 1)
                ,Tuple3.of("key1_c", "key2_", 1)
        ));

        source.countWindowAll(2)
                .sum(2).print();

        env.execute("windowAllOnDataStream");

    }

    public static void windowAllOnKeyedStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Integer>> source = env.fromCollection(Arrays.asList(
                Tuple3.of("key1_a", "key2_", 1)
                ,Tuple3.of("key1_a", "key2_", 1)
                ,Tuple3.of("key1_a", "key2_", 1)
                ,Tuple3.of("key1_b", "key2_", 1)
                ,Tuple3.of("key1_b", "key2_", 1)
                ,Tuple3.of("key1_c", "key2_", 1)
        ));

        source.
                keyBy(0)
                .countWindowAll(3)
                .sum(2).print();

        env.execute("windowAllOnDataStream");
    }


}
