package com.monkey.flink.example.statemachine.dfa;

import com.monkey.flink.example.statemachine.event.EventType;

/**
 * EventTypeAndState
 *
 * @author yong.han
 * 2019/1/25
 */
public class EventTypeAndState {

    public final EventType eventType;
    public final State state;

    public EventTypeAndState(EventType eventType, State state) {
        this.eventType = eventType;
        this.state = state;
    }
}
