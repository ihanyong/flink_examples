package com.monkey.flink.example.table.orders;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

/**
 * OrderTableSource
 *
 * @author yong.han
 * 2019/3/7
 */
public class OrderTableSource implements StreamTableSource<Order> {

//    long orderId, String comment, String owner, String shop, double amount, Date orderTime

    private static final Tuple2[] orders = new Tuple2[]{
            Tuple2.of(0L, new Order(0, "", "owner_1", "shop_1", 300.1, null))
            ,Tuple2.of(0L, new Order(0, "", "owner_1", "shop_1", 300.1, null))
    };


    @Override
    public DataStream<Order> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(new RichSourceFunction<Order>() {
            private boolean running = true;
            private long orderIdOffset = 1;
            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                while (running) {
                    for (Tuple2 orderTuple : orders) {
                        Long delay = (Long) orderTuple.f0;
                        Order order = ((Order) orderTuple.f1).clone();

                        order.setOrderId(orderIdOffset++);

                        // 引入 100 ms 的延时乱序
                        order.setOrderTime(new Date(System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(100)));

                        if (delay > 0) {
                            Thread.sleep(delay);
                        }
                        ctx.collectWithTimestamp(order, order.getOrderTime().getTime());
                    }
                    // 不break, 模拟一个无界流
//                    break;
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
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
        return "OrderTableSource";
    }
}