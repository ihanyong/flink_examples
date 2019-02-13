package com.monkey.flink.example.consecutivewindowe;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * ConsecutiveWindowedOperationsExample
 *
 * @author yong.han
 * 2019/1/28
 */
public class ConsecutiveWindowedOperationsExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        DataStream<Event> input = env.fromCollection(eventSource());


        DataStream<Event> typeSummed = input
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
                    @Override
                    public long extractAscendingTimestamp(Event element) {
                        return element.eventTime;
                    }
                })
                .keyBy(Event::getType)
                .timeWindow(Time.milliseconds(5))

                .process(new ProcessWindowFunction<Event, Event, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Event> elements, Collector<Event> out) throws Exception {
                        System.out.printf("\nstart process of sum window. key : %s, watermark: %s, windowEnd : %s============\n", key, context.currentWatermark(), context.window().getEnd());
                        int sum = 0;
                        for (Event element : elements) {
                            System.out.printf("elements ->  %s \n", element.toString());
                            sum += element.value;
                        }
                        Event e = elements.iterator().next();

                        out.collect(Event.of(e.getType(), e.id, e.eventTime, sum));

                        System.out.printf("end process of sum window. key : %s, watermark: %s, windowEnd : %s ==============\n", key, context.currentWatermark(), context.window().getEnd());
                    }
                });


        DataStream<Event> top = typeSummed.timeWindowAll(Time.milliseconds(5)).process(new ProcessAllWindowFunction<Event, Event, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Event> elements, Collector<Event> out) throws Exception {
                System.out.printf("\nstart top of sum window. windowEnd: %s, ============\n", context.window().getEnd());
                int max = 0;
                for (Event element : elements) {
                    System.out.printf("elements ->  %s \n", element.toString());
                    max = max < element.getValue() ? element.getValue() : max;
                }
                Event e = elements.iterator().next();

                out.collect(Event.of(e.getType(), e.id, e.eventTime, max));

                System.out.printf("end top of sum window. windowEnd: %s, ==============\n", context.window().getEnd());
            }
        });
        DataStream<Event> min = typeSummed.timeWindowAll(Time.milliseconds(5)).process(new ProcessAllWindowFunction<Event, Event, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Event> elements, Collector<Event> out) throws Exception {
                System.out.printf("\nstart min of sum window. windowEnd: %s, ============\n", context.window().getEnd());
                int max = 0;
                for (Event element : elements) {
                    System.out.printf("elements ->  %s \n", element.toString());
                    max = max > element.getValue() ? element.getValue() : max;
                }
                Event e = elements.iterator().next();

                out.collect(Event.of(e.getType(), e.id, e.eventTime, max));

                System.out.printf("end min of sum window. windowEnd: %s, ==============\n", context.window().getEnd());
            }
        });

//        typeSummed.print().setParallelism(1).name("sum of key and window");

        top.writeAsText("E:\\flink_data\\top").name("max");
        min.writeAsText("E:\\flink_data\\min").name("min");


//        env.execute("test consecutive windowed");
        System.out.println(env.getExecutionPlan());

    }


    private static List<Event> eventSource() {
        List<Event> list = new ArrayList<>();
        list.add(Event.of("a", 1, 0, 1));
        list.add(Event.of("a", 2, 2, 1));
        list.add(Event.of("b", 3, 4, 1));
        list.add(Event.of("a", 4, 6, 1));
        list.add(Event.of("b", 5, 9, 1));
        list.add(Event.of("a", 6, 8, 1));
        list.add(Event.of("a", 7, 9, 1));
        list.add(Event.of("a", 8, 0, 1));
        list.add(Event.of("a", 9, 11, 1));
        list.add(Event.of("a", 10, 12, 1));


        return list;
    }


    public static class Event implements Serializable {


        private String type;
        private Integer id;
        private Integer eventTime;
        private Integer value;

        public static  Event of(String type, Integer id, Integer eventTime, Integer value) {
            Event event = new Event();
            event.type = type;
            event.id = id;
            event.eventTime = eventTime;
            event.value = value;
            return event;
        }

        public String getType() {
            return type;
        }

        public Integer getId() {
            return id;
        }

        public Integer getEventTime() {
            return eventTime;
        }

        public Integer getValue() {
            return value;
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public void setEventTime(Integer eventTime) {
            this.eventTime = eventTime;
        }

        public void setValue(Integer value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "type='" + type + '\'' +
                    ", id=" + id +
                    ", eventTime=" + eventTime +
                    ", value=" + value +
                    '}';
        }
    }


}
