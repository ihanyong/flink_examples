package com.monkey.flink.example.statemachine.generator;

import com.monkey.flink.example.statemachine.event.Event;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * EventsGeneratorSource
 *
 * @author yong.han
 * 2019/1/25
 */
public class EventsGeneratorSource extends RichParallelSourceFunction<Event> {
    private final double erroProbability;
    private final int delayPerRecordMillis;
    private volatile boolean running = true;

    public EventsGeneratorSource(double erroProbability, int delayPerRecordMillis) {
        checkArgument(erroProbability>=0.0 && erroProbability <= 1.0, "error probability must be in [0.0, 1.0]");
        checkArgument(delayPerRecordMillis >= 0, "delay must be >= 0");

        this.erroProbability = erroProbability;
        this.delayPerRecordMillis = delayPerRecordMillis;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        final EventsGenerator generator = new EventsGenerator(erroProbability);

        final int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
        final int min = range * getRuntimeContext().getIndexOfThisSubtask();
        final int max = min + range;

        while (running) {
            ctx.collect(generator.next(max, min));
            if (delayPerRecordMillis > 0) {
                Thread.sleep(delayPerRecordMillis);
            }
        }

    }

    @Override
    public void cancel() {
        this.running = false;
    }



}
