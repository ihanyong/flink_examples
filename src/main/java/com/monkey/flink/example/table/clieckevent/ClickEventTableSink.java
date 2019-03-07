package com.monkey.flink.example.table.clieckevent;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;

/**
 * ClickEventTableSink
 *
 * @author yong.han
 * 2019/3/7
 */
public class ClickEventTableSink implements RetractStreamTableSink<ClickReport> {
    @Override
    public TypeInformation<ClickReport> getRecordType() {
        return TypeInformation.of(ClickReport.class);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, ClickReport>> dataStream) {
        dataStream.print();
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, ClickReport>> getOutputType() {
        return new TupleTypeInfo<>(TypeInformation.of(Boolean.class), TypeInformation.of(ClickReport.class));
    }

    @Override
    public String[] getFieldNames() {
        return new String[]{"user", "clickCount"};
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation[]{Types.STRING, Types.LONG};
    }

    @Override
    public TableSink<Tuple2<Boolean, ClickReport>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new ClickEventTableSink();
    }
}