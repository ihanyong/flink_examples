package wikiedits;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * HotItems
 *
 * @author yong.han
 * 2019/1/22
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));


        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);

        String[] fieldOrder = new String[]{"userId","itemId","categoryId","behavior","timestamp"};

        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);
        DataStream<UserBehavior> dataSource = env.createInput(csvInput, TypeInformation.of(UserBehavior.class));

        DataStream<UserBehavior> timedData = dataSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.timestamp * 1000;
            }
        });

        DataStream<UserBehavior> pvData = timedData.filter(e -> e.behavior.equals("pv"));

        DataStream<ItemViewCount> windowedData = pvData.keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());

        DataStream<String> topItems = windowedData.keyBy("windowEnd")
                .process(new TopNHotItems(3));

        topItems.print();

        env.execute("Hot Items Job");

    }


    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) {
            Long itemId = ((Tuple1<Long>) tuple).f0;
            Long count = input.iterator().next();
            out.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        private ListState<ItemViewCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ListStateDescriptor<ItemViewCount> desc = new ListStateDescriptor<>("itemState-state", ItemViewCount.class);
            itemState = getRuntimeContext().getListState(desc);
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount itemViewCount : itemState.get()) {
                allItems.add(itemViewCount);
            }

            itemState.clear();

            allItems.sort((o1, o2) -> (int) (o2.viewCount - o1.viewCount));


            StringBuilder result = new StringBuilder();
            result.append("=========================================\n");
            result.append("时间：").append(new Date(timestamp)).append("\n");
            for (int i = 0; i < topSize; i++) {
                if (i >= allItems.size()) {
                    break;
                }
                ItemViewCount current = allItems.get(i);
                result.append("No").append(i).append(":")
                        .append("商品ID=").append(current.itemId)
                        .append("浏览量=").append(current.viewCount)
                        .append("\n");

            }
            result.append("");
            result.append("");
            result.append("=========================================\n");
            out.collect(result.toString());

        }
    }




    public static class UserBehavior {
        public long userId;
        public long itemId;
        public int categoryId;
        public String behavior;
        public long timestamp;
    }

    public static class ItemViewCount {
        public long itemId;
        public long windowEnd;
        public long viewCount;

        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount t = new ItemViewCount();
            t.itemId = itemId;
            t.windowEnd = windowEnd;
            t.viewCount = viewCount;

            return t;
        }
    }
}
