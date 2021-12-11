package com.flink.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class MyKafkaUtil {

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>("localhost:9093",
                topic,
                new SimpleStringSchema());
    }
}
