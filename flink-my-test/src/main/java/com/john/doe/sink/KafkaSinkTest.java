package com.john.doe.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Created by JOHN_DOE on 2023/7/13.
 */
public class KafkaSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.fromElements("1", "2", "3", "4");

        Properties props = new Properties();

        stream.map(s -> s + "-String")
                .addSink(new FlinkKafkaProducer<String>("test1", new SimpleStringSchema(), props))
                .name("Test Sink");

        env.execute();
    }
}
