package com.monkey.flink.example.windowing;

import com.monkey.flink.example.wordcount.Tokenizer;
import com.monkey.flink.example.wordcount.WordCountData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

/**
 * WindowWordCount
 *
 * @author yong.han
 * 2019/1/25
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            text = env.fromElements(WordCountData.WORDS);
        }

        int windowSize = params.getInt("window", 10);
        int slideSize = params.getInt("slide", 5);

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .keyBy(0)
                        .countWindow(windowSize, slideSize)
//                        .timeWindow(seconds(windowSize), seconds(slideSize))
                        .sum(1);

        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            counts.print();
        }

        env.execute("windowWordCount");
    }
}
