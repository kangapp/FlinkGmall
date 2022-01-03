package com.flink.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    private static String brokers = "localhost:9093";
    private static String default_topic = "DWD_DEFAULT_TOPIC";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(brokers,
                topic,
                new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer (KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        return new FlinkKafkaProducer<T>(default_topic,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.NONE); //TODO 开启checkpoint会报错
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
        consumer.setStartFromLatest();
        return consumer;
    }

    //拼接 Kafka 相关属性到 DDL
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
                " 'topic' = '"+topic+"'," +
                " 'properties.bootstrap.servers' = '"+ brokers +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset' ";
        return ddl;
    }
}
