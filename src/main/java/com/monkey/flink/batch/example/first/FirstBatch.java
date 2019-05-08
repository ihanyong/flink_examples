package com.monkey.flink.batch.example.first;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;

/**
 * FirstBatch
 *
 * @author yong.han
 * 2019/4/9
 */
public class FirstBatch {
    public static void main(String[] args) throws Exception {
        sum();
    }

    public static void sum() throws Exception {

        Collection<Integer> collection = new ArrayList<>();
        collection.add(1);
        collection.add(2);
        collection.add(3);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> source = env.fromCollection(collection, TypeInformation.of(Integer.class));


        source
//                .map(Tuple1::of).returns(TypeInformation.of(new TypeHint<Tuple1<Integer>>() {}))
                .map(new RichMapFunction<Integer, Tuple1< Integer>>() {
                    @Override
                    public Tuple1<Integer> map(Integer value) throws Exception {
                        return Tuple1.of( value);
                    }
                })
//                .groupBy(0)
                .sum(0).print();

//        System.out.println(wordCount);

    }



    public static void wordcount() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> source = env.fromElements("Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

        DataSet<Tuple2<String, Long>> wordCount = source.flatMap(new RichFlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] vs = value.split(" ");
                for (String v : vs) {
                    out.collect(Tuple2.of(v, 1L));
                }
            }
        }).groupBy(0).sum(1);

        wordCount.print();

//        env.execute();
    }
}
