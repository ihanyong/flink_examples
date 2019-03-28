package com.monkey.flink.example.table.orders.join;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;

import java.util.Collections;
import java.util.List;

/**
 * UserStreamTableSource
 *
 * @author yong.han
 * 2019/3/19
 */
public class UserStreamTableSource implements DefinedRowtimeAttributes, StreamTableSource<User> {
    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
//        return Collections.singletonList(new RowtimeAttributeDescriptor("updateTimestamp", new ExistingField("updateTime"), new AscendingTimestamps()));
//        return Collections.singletonList(new RowtimeAttributeDescriptor("updateTimestamp", new StreamRecordTimestamp(), new AscendingTimestamps()));
        return Collections.singletonList(new RowtimeAttributeDescriptor("updateTimestamp", new StreamRecordTimestamp(), new PreserveWatermarks()));
    }


    @Override
    public DataStream<User> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(new UserStreamSource());
    }

    @Override
    public TypeInformation<User> getReturnType() {
        return TypeInformation.of(User.class);
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.fromTypeInfo(getReturnType());
    }

    @Override
    public String explainSource() {
        return"UserStreamTableSource";
    }
}
