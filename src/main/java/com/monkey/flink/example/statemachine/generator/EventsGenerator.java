package com.monkey.flink.example.statemachine.generator;

import com.monkey.flink.example.statemachine.dfa.EventTypeAndState;
import com.monkey.flink.example.statemachine.dfa.State;
import com.monkey.flink.example.statemachine.event.Event;
import com.monkey.flink.example.statemachine.event.EventType;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * EventsGenerator
 *
 * @author yong.han
 * 2019/1/25
 */
public class EventsGenerator {
    private final double errorProb;
    private final Random rnd;
    private final LinkedHashMap<Integer, State> states;

    public EventsGenerator(double errorProbability) {
        Preconditions.checkArgument(errorProbability >= 0.0 && errorProbability <= 1.0, "Invalid error probability");

        this.errorProb = errorProbability;
        this.rnd = new Random();
        this.states = new LinkedHashMap<>();

    }

    public Event next(int maxIp, int minIp) {
        final double p = rnd.nextFloat();
        if (p * 1000 >= states.size()) {
            final int nextIP = rnd.nextInt(maxIp - minIp) + minIp;

            if (!states.containsKey(nextIP)) {
                EventTypeAndState eventTypeAndState = State.Initial.randomTransition(rnd);
                states.put(nextIP, eventTypeAndState.state);
                return new Event(eventTypeAndState.eventType, nextIP);

            } else {
                return next(maxIp, minIp);
            }

        } else {

            int numToSkip = Math.min(20, rnd.nextInt(states.size()));
            Iterator<Map.Entry<Integer, State>> iter = states.entrySet().iterator();
            for (int i = numToSkip; i > 0; --i) {
                iter.next();
            }

            Map.Entry<Integer, State> entry = iter.next();
            State currentState = entry.getValue();
            int address = entry.getKey();
            iter.remove();

            if (p < errorProb) {
                EventType eventType = currentState.randomInvalidTransition(rnd);
                return new Event(eventType, address);
            } else {
                EventTypeAndState eventTypeAndState = currentState.randomTransition(rnd);
                if (!eventTypeAndState.state.isTerminal()) {
                    states.put(address, eventTypeAndState.state);
                }
                return new Event(eventTypeAndState.eventType, address);
            }
        }
    }
}

