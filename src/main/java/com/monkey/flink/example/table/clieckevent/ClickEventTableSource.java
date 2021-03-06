package com.monkey.flink.example.table.clieckevent;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * ClickEventTableSource
 *
 * @author yong.han
 * 2019/3/7
 */
public class ClickEventTableSource implements StreamTableSource<ClickEvent>, DefinedRowtimeAttributes {
//public class ClickEventTableSource implements StreamTableSource<ClickEvent> {
    @Override
    public DataStream<ClickEvent> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(new RichSourceFunction<ClickEvent>() {
            boolean running = true;

            String[] users = new String[]{"bob", "tom", "andi", "lily", "lucy"};
            Random rnd = new Random();

            @Override
            public void run(SourceContext<ClickEvent> ctx) throws Exception {
                while (running) {
                    int r = rnd.nextInt(1000);

                    String user = users[r % users.length];
                    Timestamp cTime = new Timestamp(System.currentTimeMillis());

                    ClickEvent event = new ClickEvent();
                    event.setUser(user);
                    event.setcTime(cTime);

                    ctx.collect(event);

                    Thread.sleep(r);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
    }

    @Override
    public TypeInformation<ClickEvent> getReturnType() {
        return TypeInformation.of(ClickEvent.class);
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.fromTypeInfo(getReturnType());
    }

    @Override
    public String explainSource() {
        return "ClickEventTableSource";
    }

//    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return Collections.singletonList(new RowtimeAttributeDescriptor("cTime", new ExistingField("cTime"), new AscendingTimestamps()));
    }

}