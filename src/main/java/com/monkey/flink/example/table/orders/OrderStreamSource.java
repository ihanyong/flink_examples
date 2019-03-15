package com.monkey.flink.example.table.orders;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Timestamp;
import java.util.concurrent.ThreadLocalRandom;

/**
 * OrderStreamSource
 *
 * @author yong.han
 * 2019/3/8
 */
public class OrderStreamSource extends RichSourceFunction<Order> {


//    long orderId, String comment, String owner, String shop, double amount, Date orderTime

//    private static final Tuple2[] orders = new Tuple2[]{
//            Tuple2.of(0L, new Order(0, "", "owner_1", "shop_1", 300.1, null))
//            ,Tuple2.of(0L, new Order(0, "", "owner_2", "shop_1", 20, null))
//            ,Tuple2.of(0L, new Order(0, "", "owner_3", "shop_6", 18, null))
//            ,Tuple2.of(0L, new Order(0, "", "owner_4", "shop_2", 9000, null))
//            ,Tuple2.of(0L, new Order(0, "", "owner_5", "shop_1", 60, null))
//            ,Tuple2.of(0L, new Order(0, "", "owner_1", "shop_2", 59, null))
//            ,Tuple2.of(0L, new Order(0, "", "owner_1", "shop_1", 47, null))
//            ,Tuple2.of(0L, new Order(0, "", "owner_3", "shop_4", 9.9, null))
//            ,Tuple2.of(0L, new Order(0, "", "owner_1", "shop_1", 5.8, null))
//    };
    private static final Tuple2[] orders = new Tuple2[]{
            Tuple2.of(0L, new Order(0, "", "owner_1", "shop_1", 300, null))
            ,Tuple2.of(0L, new Order(0, "", "owner_1", "shop_1", 100, null))
//            ,Tuple2.of(0L, new Order(0, "", "owner_2", "shop_1", 20, null))
    };




    private boolean running = true;
    private long orderIdOffset = 1;
    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        while (running) {
            for (Tuple2 orderTuple : orders) {
                Long delay = (Long) orderTuple.f0;
                Order order = ((Order) orderTuple.f1).clone();

                order.setOrderId(orderIdOffset++);
                order.setComment("order comment of " + order.getOrderId());

                // 引入 延时乱序
//                order.setOrderTime(new Timestamp(System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(100)));
                order.setOrderTime(System.currentTimeMillis() - delay);

                Thread.sleep(10);

                ctx.collect(order);

//                ctx.collectWithTimestamp(order, order.getOrderTime().getTime());
//                ctx.emitWatermark(new Watermark(order.getOrderTime().getTime() - 100));

            }
            // 不break, 模拟一个无界流
//                    break;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}