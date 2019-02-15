package com.monkey.flink.example.joins;

import com.monkey.flink.example.utils.ThrottledIterator;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/**
 * WindowJoinExample
 *
 * @author yong.han
 * 2019/2/14
 */
public class WindowJoinExample {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        DataStream<Tuple2<String, Integer>> salaryData =
                env.fromCollection(new ThrottledIterator<>(new SampleData.SalarySource(),2),
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        DataStream<Tuple2<String, Integer>> gradeData =
                env.fromCollection(new ThrottledIterator<>(new SampleData.GradeSource(),3),
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));



        DataStream<Tuple3<String, Integer, Integer>> joinedData =
                gradeData.join(salaryData)
                        .where(t -> t.f0)
                        .equalTo(t->t.f0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .apply((first, second)-> Tuple3.of(first.f0, first.f1, second.f1), TypeInformation.of(new TypeHint<Tuple3<String, Integer, Integer>>() {}));

        joinedData.print().setParallelism(1);

        env.execute("join example");


    }




    public static class SampleData {
        static final String[] names = {"lucy", "tom", "jam", "tang"};
        static final int grade_count = 5;
        static final int salary_max = 300000;

        public static class GradeSource implements Iterator<Tuple2<String, Integer>>, Serializable {

            private Random rnd = new Random();

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Tuple2<String, Integer> next() {
                return Tuple2.of(names[rnd.nextInt(names.length)], rnd.nextInt(grade_count) + 1);
            }
        }

        public static class SalarySource implements Iterator<Tuple2<String, Integer>>, Serializable {

            private Random rnd = new Random();

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Tuple2<String, Integer> next() {
                return Tuple2.of(names[rnd.nextInt(names.length)], rnd.nextInt(salary_max) + 1);
            }
        }
    }
}




