package com.monkey.flink.example.cep;

import com.monkey.flink.example.table.orders.Order;
import com.monkey.flink.example.table.orders.OrderStreamSource;
import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * CEPExample
 *
 * @author yong.han
 * 2019/3/27
 */
public class CEPExample {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Order> orderSource = env.addSource(new OrderStreamSource()).name("orderSource").uid("orderSource");


        Pattern<Order, ?> pattern = Pattern.<Order>begin("start").where(new SimpleCondition<Order>() {
            @Override
            public boolean filter(Order value) throws Exception {
                return value.getOwner().equals("owner_1");
            }
        }).times(4)
//                .next("next")
                .followedBy("followedBy")
                .where(new SimpleCondition<Order>() {
            @Override
            public boolean filter(Order value) throws Exception {
                return value.getOwner().equals("owner_1");
            }
        }).within(Time.seconds(15));


        PatternStream<Order> patternStream = CEP.pattern(orderSource, pattern);


        OutputTag<TimeOutAlert> outputTag = new OutputTag<TimeOutAlert>("outside", TypeInformation.of(TimeOutAlert.class));

        SingleOutputStreamOperator<Alert> resultStream = patternStream.select(
                outputTag,
                new PatternTimeoutFunction<Order, TimeOutAlert>() {
                    @Override
                    public TimeOutAlert timeout(Map<String, List<Order>> pattern, long timeoutTimestamp) throws Exception {
                        return TimeOutAlert.of("timeout pattern");
                    }
                },new PatternSelectFunction<Order, Alert>() {
                    @Override
                    public Alert select(Map<String, List<Order>> pattern) throws Exception {
                        System.out.println(pattern);

                        Alert alert = new Alert();
                        alert.setOwnerId("test1111");
                        return alert;
                    }
                });


        resultStream.getSideOutput(outputTag).print();
        resultStream.print();


        env.execute("test");

    }


    @Data
    public static class Alert {
        private String ownerId;
    }

    @Data
    public static class TimeOutAlert {
        public static TimeOutAlert of (String msg) {
            TimeOutAlert t = new TimeOutAlert();
            t.setMsg(msg);
            return t;
        }
        private String msg;
    }
}
