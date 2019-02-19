package com.monkey.flink.example.keyAndKey;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * KeybyOnKeyedStreamExample
 *
 * 得到结果：
 * keyBy on KeyedStream , 是以最后一个keyed 为准备对整个流再次应用 keyBy 算子，
 * 不是在前一次 keyed 出来的分支上分别应用 keyBy
 * @author yong.han
 * 2019/2/19
 */
public class KeybyOnKeyedStreamExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Integer>> source = env.fromCollection(Arrays.asList(
                Tuple3.of("key1_a", "key2_a", 1)
                ,Tuple3.of("key1_a", "key2_a", 1)
                ,Tuple3.of("key1_b", "key2_a", 1)
                ,Tuple3.of("key1_b", "key2_c", 1)
                ,Tuple3.of("key1_b", "key2_b", 1)
                ,Tuple3.of("key1_b", "key2_b", 1)
        ));

        source
                .keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                        return value.f0 + ":" + value.f1;
                    }
                })
                .keyBy(1)
                .reduce(new RichReduceFunction<Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                }).print();


        env.execute("keyBy on KeyedStream example");

    }





}
