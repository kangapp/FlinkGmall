package com.flink.utils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class MyKafkaUtil {

    private static String brokers = "hadoop100:9092";
    private static String DEFAULT_TOPIC = "default";
    private static Properties properties = new Properties();

    static{
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId){

        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }

            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if(record != null && record.value() != null) {
                    return new String(record.value(), StandardCharsets.UTF_8);
                }
                return null;
            }
        },properties);
        consumer.setStartFromLatest();
        return consumer;
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {

        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 15 * 1000 + "");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(DEFAULT_TOPIC, new KafkaSerializationSchema<String>() {

            @Override
            public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(topic, jsonStr.getBytes());
            }
        }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return producer;
    }
}
