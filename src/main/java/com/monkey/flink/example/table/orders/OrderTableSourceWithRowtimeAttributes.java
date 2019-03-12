package com.monkey.flink.example.table.orders;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * OrderTableSourceWithRowtimeAttributes
 *
 * @author yong.han
 * 2019/3/7
 */
public class OrderTableSourceWithRowtimeAttributes extends OrderTableSourceWithoutRowtimeAttributes implements DefinedRowtimeAttributes {

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return Collections.singletonList(new RowtimeAttributeDescriptor("orderTime", new ExistingField("orderTime"), new AscendingTimestamps()));
    }
    @Override
    public String explainSource() {
        return "OrderTableSourceWithRowtimeAttributes";
    }
}