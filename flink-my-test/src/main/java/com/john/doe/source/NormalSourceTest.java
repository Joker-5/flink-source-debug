package com.john.doe.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by JOHN_DOE on 2023/7/15.
 */
public class NormalSourceTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.addSource(new RandomWordCountSource());

        ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(
                            String value,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .shuffle()
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        return value.f0.startsWith("a");
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();

        System.out.println(env.getStreamGraph().getStreamingPlanAsJSON());
        System.out.println(env.getStreamGraph().getJobGraph().toString());
        System.out.println(env.getExecutionPlan());

        env.execute();
    }
}
