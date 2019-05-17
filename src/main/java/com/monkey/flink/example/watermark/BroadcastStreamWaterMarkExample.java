package com.monkey.flink.example.watermark;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * BroadcastStreamWaterMarkExample
 *
 * @author yong.han
 * 2019/5/17
 */
@Slf4j
public class BroadcastStreamWaterMarkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<Integer, Integer>> eventSource = env.addSource(new SourceFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
                for (int i = 0; i < Integer.MAX_VALUE; i++) {
                    Thread.sleep(500);

                    ctx.collectWithTimestamp(Tuple2.of(i, i), i);
                    ctx.emitWatermark(new Watermark(i - 1));
                }
            }

            @Override
            public void cancel() {

            }
        })
                .process(new ProcessFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public void processElement(Tuple2<Integer, Integer> value, Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        log.info("{}> eventSource              : currentWatermark: {}, timestamp: {}; {}", getRuntimeContext().getIndexOfThisSubtask(), ctx.timerService().currentWatermark(), ctx.timestamp(), value);
                        out.collect(value);
                    }
                });

        DataStream<Tuple2<Integer, Integer>> broadcastSource = env.addSource(new SourceFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
//                ctx.emitWatermark(Watermark.MAX_WATERMARK);

                for (int i = 0; i < Integer.MAX_VALUE; i++) {
                    Thread.sleep(1000);

                    ctx.collect(Tuple2.of(i, i % 3));
//                    ctx.collectWithTimestamp(Tuple2.of(i, i % 3), i);
//                    ctx.emitWatermark(new Watermark(i - 1));
                }
            }


            @Override
            public void cancel() {

            }
        })
                .process(new ProcessFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public void processElement(Tuple2<Integer, Integer> value, Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        log.info("{}> broadcastSource          : currentWatermark: {}, timestamp: {}; {}", getRuntimeContext().getIndexOfThisSubtask(), ctx.timerService().currentWatermark(), ctx.timestamp(), value);
                        out.collect(value);
                    }
                });


        MapStateDescriptor<Integer, Integer> mapStateDescriptor = new MapStateDescriptor<>("broadcastState", Integer.class, Integer.class);
        BroadcastStream<Tuple2<Integer, Integer>> broadcastStream = broadcastSource.broadcast(mapStateDescriptor);


        eventSource.connect(broadcastStream).process(new BroadcastProcessFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public void processElement(Tuple2<Integer, Integer> value, ReadOnlyContext ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                log.info("{}> processElement           : currentWatermark: {}, timestamp: {}; {}", getRuntimeContext().getIndexOfThisSubtask(), ctx.currentWatermark(), ctx.timestamp(), value);

                out.collect(value);
            }


            @Override
            public void processBroadcastElement(Tuple2<Integer, Integer> value, Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                log.info("{}> processBroadcastElement  : currentWatermark: {}, timestamp: {}; {}", getRuntimeContext().getIndexOfThisSubtask(), ctx.currentWatermark(), ctx.timestamp(), value);

            }
        })
                .process(new ProcessFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public void processElement(Tuple2<Integer, Integer> value, Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        log.warn("{}> broadcastedStream1       : currentWatermark: {}, timestamp: {}; {}", getRuntimeContext().getNumberOfParallelSubtasks(), ctx.timerService().currentWatermark(), ctx.timestamp(), value);
                        out.collect(value);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Integer, Integer> element) {
                        return element.f0+100;
                    }
                })
                .process(new ProcessFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public void processElement(Tuple2<Integer, Integer> value, Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        log.warn("{}> broadcastedStream2       : currentWatermark: {}, timestamp: {}; {}", getRuntimeContext().getNumberOfParallelSubtasks(), ctx.timerService().currentWatermark(), ctx.timestamp(), value);
                        out.collect(value);
                    }
                })


                .timeWindowAll(Time.milliseconds(5))
                .max(1)
                .print();

        env.execute();
//        System.out.println(env.getExecutionPlan());

    }


}
