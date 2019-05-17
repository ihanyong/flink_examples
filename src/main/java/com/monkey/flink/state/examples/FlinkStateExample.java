package com.monkey.flink.state.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FlinkStateExample
 *
 * @author yong.han
 * 2019/5/8
 */
public class FlinkStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String delimiter = ",";

        String input = "E:\\flink_data\\color_shape_data.csv";
        String outputOfColorCount = "E:\\flink_data\\outputOfColorCount.csv";
        String outputOfShapeCount = "E:\\flink_data\\outputOfShapeCount.csv";

        DataStream<ColorShape> colorShapeSource = env.readTextFile(input).name("file source")
                .map(new RichMapFunction<String, ColorShape>() {
                    @Override
                    public ColorShape map(String value) throws Exception {
                        return ColorShape.of(value, delimiter);
                    }
                }).name("map CSV line to ColorShape Object").uid("line2ColorShapeObject");


        colorShapeSource.keyBy("color")
                .map(new MapFunction<ColorShape, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(ColorShape value) throws Exception {
                        return Tuple2.of(value.getColor().name(), 1L);
                    }
                }).name("ColorShape2Tuple2_countColor").uid("ColorShape2Tuple2_countColor")
                .keyBy(0).sum(1).name("countColor").uid("countColor")
                .map(new MapFunction<Tuple2<String, Long>, Object>() {
                    @Override
                    public Object map(Tuple2<String, Long> value) throws Exception {
                        return value.f0 + delimiter + value.f1;
                    }
                }).name("mapTuple2ToLine_countColor").uid("mapTuple2ToLine_countColor")
                .writeAsText(outputOfColorCount);


        colorShapeSource.keyBy("shape")
                .map(new MapFunction<ColorShape, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(ColorShape value) throws Exception {
                        return Tuple2.of(value.getShape().name(), 1L);
                    }
                }).name("ColorShape2Tuple2_countShape").uid("ColorShape2Tuple2_countShape")
                .keyBy(0).sum(1).name("countShape").uid("countShape")
                .map(new MapFunction<Tuple2<String, Long>, Object>() {
                    @Override
                    public Object map(Tuple2<String, Long> value) throws Exception {
                        return value.f0 + delimiter + value.f1;
                    }
                }).name("mapTuple2ToLine_countShape").uid("mapTuple2ToLine_countShape")
                .writeAsText(outputOfShapeCount);


        env.execute("");

    }
}
