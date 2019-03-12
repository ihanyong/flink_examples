package com.monkey.flink.example.table.orders;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;

import java.util.Collections;
import java.util.List;

/**
 * OrderTableSourceWithRowtimeAttributes
 *
 * @author yong.han
 * 2019/3/7
 */
public class OrderTableSourceWithoutRowtimeAttributes implements StreamTableSource<Order> {

    @Override
    public DataStream<Order> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(new OrderStreamSource());
    }

    @Override
    public TypeInformation<Order> getReturnType() {
        return TypeInformation.of(Order.class);
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.fromTypeInfo(getReturnType());
    }
    @Override
    public String explainSource() {
        return "OrderTableSourceWithoutRowtimeAttributes";
    }
}