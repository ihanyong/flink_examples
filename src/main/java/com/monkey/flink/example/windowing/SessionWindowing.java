package com.monkey.flink.example.windowing;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * SessionWindowing
 *
 * @author yong.han
 * 2019/1/25
 */
public class SessionWindowing {
    private static List<Tuple3<String, Long, Integer>> getInput(){
        List<Tuple3<String, Long, Integer>> input = new ArrayList<>();
        input.add(new Tuple3<>("a", 1L, 1));
        input.add(new Tuple3<>("b", 1L, 1));
        input.add(new Tuple3<>("b", 3L, 1));
        input.add(new Tuple3<>("b", 5L, 1));
        input.add(new Tuple3<>("c", 6L, 1));
        // We expect to detect the session "a" earlier than this point (the old
        // functionality can only detect here when the next starts)
        input.add(new Tuple3<>("a", 10L, 1));
        // We expect to detect session "b" and "c" at this point as well
        input.add(new Tuple3<>("c", 11L, 1));
        return input;
    }



    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime.EventTime);
        env.setParallelism(1);

        List<Tuple3<String, Long, Integer>> input = getInput();
        DataStream<Tuple3<String, Long, Integer>> source = env
                .addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
                    @Override
                    public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                        for (Tuple3<String, Long, Integer> value : input) {
                            ctx.collectWithTimestamp(value, value.f1);
                            ctx.emitWatermark(new Watermark(value.f1 - 1));
                        }
                        ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
                    }
                    @Override
                    public void cancel() { }
                });

        DataStream<Tuple3<String, Long, Integer>> aggregated = source
                .keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
                .sum(2);


        if (params.has("output")) {
            aggregated.writeAsText(params.get("output"));
        } else {
            aggregated.print();
        }

        env.execute("SessionWindowing Example");

    }

}
