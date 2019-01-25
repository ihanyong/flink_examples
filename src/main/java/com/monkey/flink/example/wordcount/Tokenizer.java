package com.monkey.flink.example.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Tokenizer
 *
 * @author yong.han
 * 2019/1/25
 */
public class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] tokens = value.toLowerCase().split("\\W+");

        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(Tuple2.of(token, 1));
            }
        }
    }
}
