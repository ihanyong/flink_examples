package com.monkey.flink.example.statemachine.event;

/**
 * Event
 *
 * @author yong.han
 * 2019/1/25
 */
public class Event {
    private final EventType type;
    private final int sourceAddress;

    public Event(EventType type, int sourceAddress) {
        this.type = type;
        this.sourceAddress = sourceAddress;
    }

    public EventType type() {
        return this.type;
    }

    public int sourceAddress() {
        return sourceAddress;
    }

    @Override
    public int hashCode() {
        return 31 * type.hashCode() + sourceAddress;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        } else {
            final Event that = (Event) obj;
            return this.type == that.type && this.sourceAddress == that.sourceAddress;
        }
    }

    @Override
    public String toString() {
        return "Event " + formatAddress(sourceAddress) + " : " + type.name();
    }

    public static String formatAddress(int address) {
        int b1 = (address >>> 24) & 0xff;
        int b2 = (address >>> 16) & 0xff;
        int b3 = (address >>>  8) & 0xff;
        int b4 = address          & 0xff;

        return "" + b1 + "." + b2 + "." + b3 + "." + b4;
    }
}
