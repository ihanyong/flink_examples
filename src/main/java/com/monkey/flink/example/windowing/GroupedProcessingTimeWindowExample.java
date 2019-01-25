package com.monkey.flink.example.windowing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import static org.apache.flink.streaming.api.windowing.time.Time.milliseconds;

/**
 * GroupedProcessingTimeWindowExample
 *
 * @author yong.han
 * 2019/1/25
 */
public class GroupedProcessingTimeWindowExample {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStream<Tuple2<Long, Long>> stream = env.addSource(new DataSource());


        stream.keyBy(0)
                .timeWindow(milliseconds(2500), milliseconds(500))
                .reduce(new SumminReducer())
                .print();
//                .addSink(new SinkFunction<Tuple2<Long, Long>>() {
//                    @Override
//                    public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
//
//                    }
//                });

        env.execute("Grouped Processing Time Window Example");
    }

    private static class SumminReducer implements ReduceFunction<Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
        }
    }


    private static class DataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
        private volatile boolean running = true;


        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
            final long startTime = System.currentTimeMillis();

            final long numElements = 20_000_000;
            final long numKeys = 10_000;
            long val = 1L;
            long count = 0L;

            while (running && count < numElements) {
                count++;
                ctx.collect(Tuple2.of(val++, 1L));
                if (val > numElements) {
                    val = 1;
                }

            }
            final long endTime = System.currentTimeMillis();

            System.out.println("Took " + (endTime - startTime) + " millis for " + numElements + "values");
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }





}
