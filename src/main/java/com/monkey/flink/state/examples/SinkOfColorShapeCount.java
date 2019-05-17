package com.monkey.flink.state.examples;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * SinkOfColorShapeCount
 *
 * @author yong.han
 * 2019/5/8
 */
public class SinkOfColorShapeCount extends RichSinkFunction<String> {

    @Override
    public void invoke(String value, Context context) throws Exception {

    }
}
