package wikiedits;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * WikipediaAnalysis
 *
 * @author yong.han
 * 2019/1/15
 */
public class WikipediaAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        see.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
//        edits.assignTimestampsAndWatermarks();

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(WikipediaEditEvent::getUser);

        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(
                        new Tuple2<>("", 0L)
                        ,
                        (acc, event) -> {
                            acc.f0 = event.getUser();
                            acc.f1 += event.getByteDiff();
                            return acc;
                        }, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})
                );
//        result.print();
        result.map(Tuple2::toString)
                .addSink(new FlinkKafkaProducer011<>("192.168.6.21:9092,192.168.6.22:9092,192.168.6.23:9092", "wiki-result", new SimpleStringSchema()));

//        see.execute("test job of wikipedia analysis");
        System.out.println(see.getExecutionPlan());

    }

}
