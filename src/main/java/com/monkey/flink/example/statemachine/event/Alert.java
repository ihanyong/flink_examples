package com.monkey.flink.example.statemachine.event;

import com.monkey.flink.example.statemachine.dfa.State;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Alert
 *
 * @author yong.han
 * 2019/1/25
 */
public class Alert {

    private final int address;
    private final State state;
    private final EventType transition;

    public Alert(int address, State state, EventType transition) {
        this.address = address;
        this.state = checkNotNull(state);
        this.transition = checkNotNull(transition);
    }

    public int getAddress() {
        return address;
    }

    public State getState() {
        return state;
    }

    public EventType getTransition() {
        return transition;
    }


    @Override
    public int hashCode() {
        int code = 31 * address + state.hashCode();
        return 31 * code + transition.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        } else {
            final Alert that = (Alert) obj;
            return this.address == that.address &&
                    this.transition == that.transition &&
                    this.state == that.state;
        }


    }

    @Override
    public String toString() {
        return "ALERT " + Event.formatAddress(address) + " : " + state.name() + " -> " + transition.name();
    }
}
