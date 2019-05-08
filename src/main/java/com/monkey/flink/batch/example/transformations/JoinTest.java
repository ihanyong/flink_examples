package com.monkey.flink.batch.example.transformations;

import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST;

/**
 * JoinTest
 *
 * @author yong.han
 * 2019/4/9
 */
public class JoinTest {
    public static void main(String[] args) {


//        invoke((persons,interests) -> {
//            try {
//                persons.leftOuterJoin(interests, REPARTITION_HASH_FIRST).where("id").equalTo("personId").with(new RichJoinFunction<Person, Interest, String>() {
//                    @Override
//                    public String join(Person first, Interest second) throws Exception {
//                        return new StringBuffer().append(first).append("'s interest is ").append(second).toString();
//                    }
//                }).print();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });


        invoke((persons,interests) -> {
            try {
                persons
//                        .rebalance()

//                        .partitionByHash("id")

//                        .partitionByRange("id")

//                        .partitionCustom(new Partitioner<Long>() {
//                            @Override
//                            public int partition(Long key, int numPartitions) {
//                                return key.intValue() % numPartitions;
//                            }
//                        }, "id")


                        .sortPartition("id", Order.DESCENDING)

//                        .mapPartition(new MapPartitionFunction<Person, String>() {
//                            @Override
//                            public void mapPartition(Iterable<Person> values, Collector<String> out) throws Exception {
//                                String r = Lists.newArrayList(values).stream().map(Person::getId).map(String::valueOf).collect(Collectors.joining(",", "[", "]"));
//                                out.collect(r);
//                            }
//                        })
                        .first(3)
                        .print();

//                persons.cross(interests).print();
//                persons.cross(interests).print();

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }


    public static void invoke(BiConsumer<DataSet<Person>,DataSet<Interest>> consumer) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Person> persons = env.fromElements(
                Person.of(1L, "hanyong")
                , Person.of(2L, "xiatian")
                , Person.of(3L, "gui")
                , Person.of(4L, "tom")
                , Person.of(5L, "jam")
                , Person.of(6L, "lucy"));

        DataSet<Interest> interests = env.fromElements(
                Interest.of(1L, "basketball")
                , Interest.of(3L, "basketball")
                , Interest.of(1L, "drink")
                , Interest.of(6L, "eat")
                , Interest.of(1L, "movies")
                , Interest.of(100L, "hahaha")
        );


        consumer.accept(persons, interests);

    }


    @Data
    public static class Person {
        private Long id;
        private String name;

        public static Person of(Long id, String name) {
            Person p = new Person();
            p.setId(id);
            p.setName(name);
            return p;
        }
    }

    @Data
    public static class Interest {
        private Long personId;
        private String interest;

        public static Interest of(Long personId, String interest) {
            Interest i = new Interest();
            i.setInterest(interest);
            i.setPersonId(personId);
            return i;
        }
    }

}

