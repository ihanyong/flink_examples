package com.wwware.flink.test.stock;

import com.alibaba.otter.canal.client.kafka.MessageDeserializer;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;

/**
 * OwnerStock
 *
 * @author yong.han
 * 2019/1/23
 */
public class OwnerStock {
    public static MessageDeserializer deserializer = new MessageDeserializer();


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<byte[]> source = getSource(env);

        DataStream<Dml> dmlDataStream = source
                .map(OwnerStock::parse4Dml)
                .flatMap(OwnerStock::flatMapDmlList)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Dml>() {
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis() - 3000);
                    }

                    @Override
                    public long extractTimestamp(Dml element, long previousElementTimestamp) {
                        return element.getEs();
                    }
                })

//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Dml>() {
//            @Override
//            public long extractAscendingTimestamp(Dml element) {
//                return element.getEs();
//            }
//        })
                ;

        DataStream<Tuple3<String, Long, TimeWindow>> sumStream = dmlDataStream
                .keyBy("table")
                .timeWindow(Time.seconds(5)).trigger(EventTimeTrigger.create())
                .aggregate(new TableUpdateCountAggregateFunction(), new TableUpdateCountWindowFunction());

        sumStream.addSink(new MysqlSink()).name("mysqlSink");


        env.execute("resource table update");

    }


    public static class TableUpdateCountAggregateFunction implements AggregateFunction<Dml, Tuple2<String, Long>, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> createAccumulator() {
            return Tuple2.of(null, 0L);
        }

        @Override
        public Tuple2<String, Long> add(Dml value, Tuple2<String, Long> accumulator) {
            return Tuple2.of(value.getTable(), accumulator.f1 + 1);
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
            return Tuple2.of(b.f0, a.f1 + b.f1);
        }

    }

    public static class TableUpdateCountWindowFunction
    implements WindowFunction<Tuple2<String, Long>, Tuple3<String,Long,TimeWindow>, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple3<String,Long,TimeWindow>> out) throws Exception {
            input.forEach(e -> out.collect(Tuple3.of(e.f0, e.f1, window)));
        }
    }





    public static List<Dml> parse4Dml(byte[] msg) {
        Message message = deserializer.deserialize(null, msg);
        return MessageUtil.parse4Dml(null, message);
    }

    public static void flatMapDmlList(List<Dml> list, Collector<Dml> out) {
        list.forEach(out::collect);
    }

    public static Tuple2<String, Long> mapDmlToOne(Dml dml) {
        return Tuple2.of(dml.getTable(), 1L);
    }

    private static DataStream<byte[]> getSource(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.6.21:9092,192.168.6.22:9092,192.168.6.23:9092");
        props.put("group.id", "flink-test");

//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", MessageDeserializer.class.getName());

        return env.addSource(new FlinkKafkaConsumer011<>("canal_resource_center", new CanalMqMessageNoTransformDeserializationSchema(), props));
    }



}

