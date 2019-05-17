package com.monkey.flink;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * WindowedAndStreamMap
 *
 * @author yong.han
 * 2019/5/15
 */
public class WindowedAndStreamMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        long current = System.currentTimeMillis();
        env.fromElements(  // (eventTime, sum)
                Tuple2.of(current + 0, 0)
                , Tuple2.of(current + 1, 1)
                , Tuple2.of(current + 2, 2)
                , Tuple2.of(current + 3, 3)
                , Tuple2.of(current + 4, 4)
                , Tuple2.of(current + 5, 5)
                , Tuple2.of(current + 6, 6)
                , Tuple2.of(current + 7, 7)
                , Tuple2.of(current + 8, 8)
                , Tuple2.of(current + 9, 9)
                , Tuple2.of(current + 10, 10)
                , Tuple2.of(current + 11, 11)
                , Tuple2.of(current + 12, 12)
                , Tuple2.of(current + 13, 3)
                , Tuple2.of(current + 14, 14)
                , Tuple2.of(current + 15, 15)
                , Tuple2.of(current + 16, 16)
                , Tuple2.of(current + 17, 17)
                , Tuple2.of(current + 18, 18)
                , Tuple2.of(current + 19, 19)
                , Tuple2.of(current + 20, 20)
                , Tuple2.of(current + 21, 21)
                , Tuple2.of(current + 22, 22)
                , Tuple2.of(current + 23, 23)
                , Tuple2.of(current + 24, 16)
                , Tuple2.of(current + 25, 25)
                , Tuple2.of(current + 26, 26)
                , Tuple2.of(current + 27, 27)
                , Tuple2.of(current + 28, 28)
                , Tuple2.of(current + 29, 29)
                , Tuple2.of(current + 30, 28)
                , Tuple2.of(current + 31, 31)

        )
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Long, Integer> element) {
                        return element.f0;
                    }
                })
                .timeWindowAll(Time.milliseconds(10))
                // max and attach window info
                .reduce(new ReduceFunction<Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> reduce(Tuple2<Long, Integer> value1, Tuple2<Long, Integer> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                }, new AllWindowFunction<Tuple2<Long, Integer>, Tuple3<String, TimeWindow, Integer>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<Long, Integer>> values, Collector<Tuple3<String, TimeWindow, Integer>> out) throws Exception {
                        out.collect(Tuple3.of("1", window, values.iterator().next().f1));
                    }
                })
                .keyBy(0) // keyBy to use key state here
                .map(new RichMapFunction<Tuple3<String, TimeWindow, Integer>, Tuple2<TimeWindow, Integer>>() {
                    // store the prev window's max
                    private transient ValueState<Integer> prev;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> prevDesc = new ValueStateDescriptor<Integer>("prev", Integer.class, 0);
                        prev = getRuntimeContext().getState(prevDesc);
                    }

                    @Override
                    public Tuple2<TimeWindow, Integer> map(Tuple3<String, TimeWindow, Integer> value) throws Exception {
                        // calculate the current window's cost and update the state of prev
                        Integer prevMax = prev.value();
                        Integer currentMax = value.f2;
                        prev.update(currentMax);
                        return Tuple2.of(value.f1, currentMax - prevMax);
                    }
                })
                .map(new MapFunction<Tuple2<TimeWindow, Integer>, Tuple3<Timestamp, Timestamp, Integer>>() {
                    @Override
                    public Tuple3<Timestamp, Timestamp, Integer> map(Tuple2<TimeWindow, Integer> value) throws Exception {
                        return Tuple3.of(new Timestamp(value.f0.getStart()), new Timestamp(value.f0.getEnd()), value.f1);
                    }

                }).print();


        env.execute();
//        System.out.println(env.getExecutionPlan());

    }
}