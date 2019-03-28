package com.monkey.flink.example.table.orders;

import org.apache.flink.table.sources.*;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;

import java.util.Collections;
import java.util.List;

/**
 * OrderTableSourceWithRowtimeAttributes
 *
 * @author yong.han
 * 2019/3/7
 */
public class OrderTableSourceWithRowtimeAttributes extends OrderTableSourceWithoutRowtimeAttributes implements DefinedRowtimeAttributes {

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return Collections.singletonList(new RowtimeAttributeDescriptor("t", new ExistingField("orderTime"), new PreserveWatermarks()));
    }
    @Override
    public String explainSource() {
        return "OrderTableSourceWithRowtimeAttributes";
    }
}