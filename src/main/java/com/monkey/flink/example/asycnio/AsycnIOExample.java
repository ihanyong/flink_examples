package com.monkey.flink.example.asycnio;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.*;

/**
 * AsycnIOExample
 *
 * @author yong.han
 * 2019/1/29
 */
public class AsycnIOExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        DataStream<Integer> input = env.addSource(new SimpleSource());


        DataStream<String> result = AsyncDataStream.orderedWait(
                input,
                new SampleAsyncFunction(),
                2000L,
                TimeUnit.MILLISECONDS);


        result.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(value, 1));
            }
        }).keyBy(0).process(new StatedSumFunction()).print();


        env.execute("AsycnIO Example");
    }

    public static class StatedSumFunction extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private transient ValueState<Integer> valueState;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("sum-value", Integer.class, 0));
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer currentSum = valueState.value() + value.f1;

            valueState.update(currentSum);

            out.collect(Tuple2.of(value.f0, currentSum));
        }
    }

    public static class SampleAsyncFunction extends RichAsyncFunction<Integer, String> {
        private transient ExecutorService executorService;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            executorService = Executors.newFixedThreadPool(30);
        }

        @Override
        public void close() throws Exception {
            super.close();
            executorService.shutdown();
        }

        @Override
        public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) throws Exception {
            executorService.submit(() -> {
                long sleep = (long) ThreadLocalRandom.current().nextLong(1000);

                try {
                    Thread.sleep(sleep);

                    if (ThreadLocalRandom.current().nextFloat() < 0.3) {
//                        System.out.println("excption");
                        resultFuture.completeExceptionally(new Exception("lalalalal"));
//                        resultFuture.complete(singleton("key-fail-" + input % 10));

                    } else {
                        resultFuture.complete(singleton("key-" + input % 2));
                    }

                } catch (InterruptedException e) {
                    resultFuture.complete(emptyList());
                }
            });


        }
    }


    public static class SimpleSource implements SourceFunction<Integer>
//            , ListCheckpointed<Integer>
    {
        private volatile boolean running = true;
        private int counter;

//        @Override
//        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
//            return Collections.singletonList(this.counter);
//        }
//
//        @Override
//        public void restoreState(List<Integer> state) throws Exception {
//            for (Integer integer : state) {
//                this.counter = integer;
//            }
//        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(counter);
                if (counter == 200) {
                    counter = 0;
                }

                Thread.sleep(10L);
            }

        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }

}
