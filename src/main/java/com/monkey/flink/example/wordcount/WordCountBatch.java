package com.monkey.flink.example.wordcount;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * WordCountBatch
 *
 * @author yong.han
 * 2019/1/25
 */
public class WordCountBatch {

    public static void main(String[] args) throws Exception{
        ParameterTool params = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);


        DataSet<String> text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            text = env.fromElements(WordCountData.WORDS);
        }


        DataSet<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .groupBy(0)
                        .sum(1);

        if ((params.has("output"))) {
            counts.writeAsCsv(params.get("output"));
        } else {
            counts.print();

        }
        env.execute("WordCountBatch");

    }
}
