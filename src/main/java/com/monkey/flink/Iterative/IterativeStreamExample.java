package com.monkey.flink.Iterative;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * IterativeStreamExample
 *
 * @author yong.han
 * 2019/5/10
 */
public class IterativeStreamExample {


    public static void main1(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<Integer,Integer, Integer>> source = env.fromElements(
               0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,100,120,160,190,199,200,201
        ).map(i -> Tuple3.of(i, i, 0)).returns(new TypeHint<Tuple3<Integer, Integer, Integer>>() {});

        IterativeStream<Tuple3<Integer, Integer, Integer>> iterative = source.iterate();

        DataStream<Tuple3<Integer, Integer, Integer>> iterativeBody =
                iterative.map(new MapFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Tuple3<Integer, Integer, Integer> map(Tuple3<Integer, Integer, Integer> value) throws Exception {
                if (value.f1 < 3) {
                    return value;
                }
                Tuple3<Integer, Integer, Integer> result = Tuple3.of(value.f0, (value.f1 - 3), value.f2 + 1);
                return result;
            }
        });

        DataStream<Tuple3<Integer, Integer, Integer>> feedback = iterativeBody.filter((FilterFunction<Tuple3<Integer, Integer, Integer>>) value -> value.f1 >= 3);

        iterative.closeWith(feedback);

        DataStream<Tuple3<Integer, Integer, Integer>> output = iterativeBody.filter((FilterFunction<Tuple3<Integer, Integer, Integer>>) value -> value.f1 < 3);


        output.print();

//        source.print();

        env.execute("");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer,Integer>> source = env.fromElements(
               0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,100,120,160,190,199,200,201
//               3
        ).map(i -> Tuple2.of(i, i)).returns(new TypeHint<Tuple2<Integer, Integer>>() {});

//        IterativeStream<Tuple2<Integer, Integer>> iterative = source.iterate();
//
//        DataStream<Tuple2<Integer, Integer>> iterativeBody =
//                iterative.map(new MapFunction<Tuple2<Integer, Integer>,Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
//                Tuple2<Integer, Integer> result = Tuple2.of(value.f0, value.f1-1);
//                return result;
//            }
//        });
//
//        DataStream<Tuple2<Integer, Integer>> feedback = iterativeBody.filter((FilterFunction<Tuple2<Integer, Integer>>) value -> value.f1 >= 3);
//
//        iterative.closeWith(feedback);
//
//        DataStream<Tuple2<Integer, Integer>> output = iterativeBody.filter((FilterFunction<Tuple2<Integer, Integer>>) value -> value.f1 < 3);


//        output.print();
        source.print();

        env.execute("");
    }
}
