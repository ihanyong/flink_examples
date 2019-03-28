package com.monkey.flink.example.table.orders.join;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * UserStreamSource
 *
 * @author yong.han
 * 2019/3/20
 */
public class UserStreamSource implements SourceFunction<User> {

    private boolean running = true;
    private long updateTime = 0;
    @Override
    public void run(SourceContext<User> ctx) throws Exception {
        int count = 0;
        while (running) {
//                    Thread.sleep(10000);
            if (count < 10) {
//                        ctx.collect(new User("owner_1", "1-username-" + updateTime, updateTime));
//                        ctx.collect(new User("owner_0", "0-username-" + updateTime, updateTime));
                User user = null;
                if (count % 3 == 0) {
                    user = new User("owner_1", "1-username-" + updateTime, updateTime);
                } else {
                    user = new User("owner_0", "0-username-" + updateTime, updateTime);
                }
                ctx.collectWithTimestamp(user,updateTime-1);
//                ctx.collect(user);
                ctx.emitWatermark(new Watermark(updateTime));

                System.out.println(">------>" + user);
            } else {
                ctx.emitWatermark(new Watermark(updateTime));
                System.out.println(">>>>>>>" + updateTime);
            }
            count++;
            updateTime += 2000;
            Thread.sleep(2000);
        }
    }
    @Override
    public void cancel() {
        this.running = false;
    }


}
