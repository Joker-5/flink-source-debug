package com.john.doe.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Properties;

/**
 * Created by JOHN_DOE on 2023/7/11.
 */
public class KafkaSourceTest {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        // props...
        Properties props = new Properties();
        env.addSource(new FlinkKafkaConsumer<>("foo", new SimpleStringSchema(), props));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.poll(Duration.ofMillis(100));
    }
}
