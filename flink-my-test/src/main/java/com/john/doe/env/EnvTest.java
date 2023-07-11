package com.john.doe.env;

import com.john.doe.entity.Person;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by JOHN_DOE on 2023/7/10.
 */
public class EnvTest {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
