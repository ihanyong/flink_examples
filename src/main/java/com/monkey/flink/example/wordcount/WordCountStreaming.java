package com.monkey.flink.example.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * WordCountStreaming
 *
 * @author yong.han
 * 2019/1/25
 */
public class WordCountStreaming {
    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WordCountStreaming example with default input data set.");
            System.out.println("Use --input to specify file input");
            text = env.fromElements(WordCountData.WORDS);
        }
        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .keyBy(0)
//                        .timeWindow(Time.seconds(2))
                        .sum(1);

        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path");
            counts.print();
        }
        env.execute("WordCountStreaming");

    }
}
