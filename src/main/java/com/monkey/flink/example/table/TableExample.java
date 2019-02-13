package com.monkey.flink.example.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * TableExample
 *
 * @author yong.han
 * 2019/2/13
 */
public class TableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(streamEnv);

        tableEnv.registerTableSource("click_event", new ClickEventTableSource());
        tableEnv.registerTableSink("result", new ClickEventTableSink());


        Table table = tableEnv.scan("click_event");
        Table result = table.groupBy("user").select("user, count(cTime) as clickCount");

        result.insertInto("result");


        streamEnv.execute("stream table example");

    }


    public static class ClickEvent {
        private String user;
        private long cTime;

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public long getcTime() {
            return cTime;
        }

        public void setcTime(long cTime) {
            this.cTime = cTime;
        }
    }

    public static class ClickReport {
        private String user;
        private Long clickCount;

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public Long getClickCount() {
            return clickCount;
        }

        public void setClickCount(Long clickCount) {
            this.clickCount = clickCount;
        }

        @Override
        public String toString() {
            return "ClickReport{" +
                    "user='" + user + '\'' +
                    ", clickCount=" + clickCount +
                    '}';
        }
    }

    public static class ClickEventTableSource implements StreamTableSource<ClickEvent> {
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
                        long cTime = System.currentTimeMillis();

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
    }

    public static class ClickEventTableSink implements RetractStreamTableSink<ClickReport> {
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



}
