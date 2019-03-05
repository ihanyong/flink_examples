package com.monkey.flink.example.stateTest;

import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * StateConcurrencyUpdataTest
 *
 * @author yong.han
 * 2019/3/4
 */
public class StateConcurrencyUpdataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<Tuple2<Long, Long>> source = env.addSource(new KeyValueSource());
        source
                .keyBy(0).process(new TestProcessFunciotn())
                .keyBy(0).process(new TestProcessFunciotn2())
                .print();



        env.execute("test state concurrency updata");
    }



    public static class TestProcessFunciotn extends KeyedProcessFunction<Tuple, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        private ValueState<Long> sum;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            sum = getRuntimeContext().getState(new ValueStateDescriptor<Long>("test-state-concurrency-update", Long.class));
        }
        @Override
        public void processElement(Tuple2<Long, Long> value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
            Long s = sum.value();
            if (s == null) {
                s = 0L;
            }
//            s += value.f1;
            s ++;
            sum.update(s);
            out.collect(Tuple2.of(value.f0, sum.value()));
        }

    }
    public static class TestProcessFunciotn2 extends KeyedProcessFunction<Tuple, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        private ValueState<Long> sum;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            sum = getRuntimeContext().getState(new ValueStateDescriptor<Long>("test-state-concurrency-update", Long.class));
        }
        @Override
        public void processElement(Tuple2<Long, Long> value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
            Long s = sum.value();
            if (s == null) {
                s = 0L;
            }
//            s += value.f1;
            s ++;
            sum.update(s);
            out.collect(Tuple2.of(value.f0, sum.value()));
        }

    }

    public static class KeyValueSource implements SourceFunction<Tuple2<Long, Long>> {
        private boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
            long count = 1;
            while (running) {
                ctx.collect(Tuple2.of(count%2, 1L));
//                ctx.collect(Tuple2.of(1L, 1L));

                if (count % 20000 == 0) {
                    Thread.sleep(2_000);
                }

                count++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

