package com.monkey.flink.example.statemachine.dfa;

import com.monkey.flink.example.statemachine.event.EventType;

import java.io.Serializable;

/**
 * Transition
 *
 * @author yong.han
 * 2019/1/25
 */
public class Transition implements Serializable {
    private static final long serialVersionUID = 1L;

    private final EventType eventType;
    private final State targetState;
    private final float prob;

    public Transition(EventType eventType, State targetState, float prob) {
        this.eventType = eventType;
        this.targetState = targetState;
        this.prob = prob;
    }

    public EventType getEventType() {
        return eventType;
    }

    public State getTargetState() {
        return targetState;
    }

    public float getProb() {
        return prob;
    }

    @Override
    public int hashCode() {
        int code = 31 * eventType.hashCode() + targetState.hashCode();
        return 31 * code + (prob != 0.0f ? Float.floatToIntBits(prob) : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        } else {
            final Transition that = (Transition) obj;
            return this.eventType == that.eventType &&
                    this.targetState == that.targetState &&
                    Float.compare(this.prob, that.prob) == 0;
        }
    }

    @Override
    public String toString() {
        return "--[" + eventType.name() + "]--> " + targetState.name() + " " + prob + ")";
    }
}
