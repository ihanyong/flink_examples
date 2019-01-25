package com.monkey.flink.example.statemachine;

import com.monkey.flink.example.statemachine.dfa.State;
import com.monkey.flink.example.statemachine.event.Alert;
import com.monkey.flink.example.statemachine.event.Event;
import com.monkey.flink.example.statemachine.generator.EventsGeneratorSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * StateMachineExample
 *
 * @author yong.han
 * 2019/1/25
 */
public class StateMachineExample {
    public static void main(String[] args) throws Exception {
        final SourceFunction<Event> source;


        final ParameterTool params = ParameterTool.fromArgs(args);

//        if (params.has("kafka-topic")) {
//
//        } else {
            double errorRate = params.getDouble("error-rate", 0.25);
            int sleep = params.getInt("sleep", 500);

            System.out.printf("Using standalone source with error rate %f and sleep delay %s millis\n", errorRate, sleep);
            System.out.println();
            source = new EventsGeneratorSource(errorRate, sleep);
//        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        env.setStateBackend(new FsStateBackend("file:///E:/flink_checkoutpoint"));


        env.getConfig().setGlobalJobParameters(params);

        DataStream<Event> events = env.addSource(source);

        DataStream<Alert> alerts = events
                .keyBy(Event::sourceAddress)
                .flatMap(new StateMachineMapper());

        alerts.print().setParallelism(1);


        env.execute("state machine job");

    }

    private static class StateMachineMapper extends RichFlatMapFunction<Event, Alert> {
        private ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            currentState = getRuntimeContext().getState(
                    new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(Event value, Collector<Alert> out) throws Exception {
            State state = currentState.value();

            if (state == null) {
                state = State.Initial;
            }

            State nextState = state.transition(value.type());

            if (nextState == State.InvalidTransition) {
                out.collect(new Alert(value.sourceAddress(), state, value.type()));
            } else if (nextState.isTerminal()) {
                currentState.clear();
            } else {
                currentState.update(nextState);
            }
        }
    }

}
