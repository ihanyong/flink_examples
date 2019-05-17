package com.monkey.flink.times;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

/**
 * ParallelWaterarksExample
 *
 * @author yong.han
 * 2019/5/10
 */
public class ParallelWaterarksExample {


    public static void main(String[] args) throws Exception {
        int times = 1;


        final Tuple2<String, Integer>[] tuple2s = new Tuple2[]{
                Tuple2.of("element000", 0)
                , Tuple2.of("element001", 0)
                , Tuple2.of("element002", 0)
                , Tuple2.of("element001", 0)
                , Tuple2.of("element004", 10)
                , Tuple2.of("element001", 11)
                , Tuple2.of("element001", 11)
                , Tuple2.of("element002", 11)
                , Tuple2.of("element002", 11)
                , Tuple2.of("element001", 12)
                , Tuple2.of("element004", 13)
                , Tuple2.of("element001", 14)
                , Tuple2.of("element001", 15)
                , Tuple2.of("element004", 16)
                , Tuple2.of("element001", 24)
                , Tuple2.of("element002", 26)
                , Tuple2.of("element003", 26)
                , Tuple2.of("element002", 27)
                , Tuple2.of("element003", 31)
                , Tuple2.of("element002", 31)
                , Tuple2.of("element002", 32)

        };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStream<Tuple2<String, Integer>> source = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
        DataStream<Tuple3<String, Integer, Integer>> source =
                env.addSource(new RichParallelSourceFunction<Tuple3<String, Integer, Integer>>() {

            @Override
            public void run(SourceContext<Tuple3<String, Integer, Integer>> ctx) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                int total = getRuntimeContext().getNumberOfParallelSubtasks();
                for (int i = 0; i < tuple2s.length; i++) {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(500));

                    if (i % total == index) {
                        Tuple2<String, Integer> t2 = tuple2s[i];
                        ctx.collect(Tuple3.of(t2.f0, t2.f1, i));
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<String, Integer, Integer> element) {
                        return element.f1;
                    }

                });
//                .setParallelism(5)


        // ||| detective parallel watermark start ============================================================================
        source.process(new ProcessFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
            @Override
            public void processElement(Tuple3<String, Integer, Integer> value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {

                ctx.timerService().currentWatermark();
                ctx.timerService().currentProcessingTime();
                ctx.timestamp();

                StringBuffer sb = new StringBuffer().append(getRuntimeContext().getIndexOfThisSubtask()+1).append("> ");
                sb.append("process log: ").append(value).append(";")
                .append("ctx.timerService().currentWatermark():").append(ctx.timerService().currentWatermark()).append(";")
                .append("ctx.timerService().currentProcessingTime():").append(ctx.timerService().currentProcessingTime()).append(";")
                .append("ctx.timestamp():").append(ctx.timestamp()).append(";")
                ;

                System.out.println(sb.toString());

                out.collect(value);
            }
        }).print();



        // ||| detective parallel watermark stop  ============================================================================




//        keyWindowAggregate(source);

        env.execute();
//        System.out.println(env.getExecutionPlan());

    }


    private static void keyWindowAggregate(DataStream<Tuple2<String, Integer>> source) {
         source

                .keyBy(0)

                .timeWindow(Time.milliseconds(10))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, StringBuffer, String>() {
                    @Override
                    public StringBuffer createAccumulator() {
                        return new StringBuffer();
                    }

                    @Override
                    public StringBuffer add(Tuple2<String, Integer> value, StringBuffer accumulator) {
                        return accumulator.append(value).append(";");
                    }

                    @Override
                    public String getResult(StringBuffer accumulator) {
                        return accumulator.toString();
                    }

                    @Override
                    public StringBuffer merge(StringBuffer a, StringBuffer b) {
                        return a.append(b);
                    }
                })
                .print();
    }
}
