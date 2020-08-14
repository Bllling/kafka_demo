package com.lin.kafka_demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;

public class Producer {
    private static final String brokerList = "192.168.18.128:9092";
    private static final String topic = "lin";
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置key序列化器
//        properties.put("key.serializer",
//                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置重复次数
        properties.put(ProducerConfig.RETRIES_CONFIG,10);
        // 设置值序列化
//        properties.put("value.serializer",
//                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //设置集群地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, "kafka_demo", "我爱娅娅");
        kafkaProducer.send(producerRecord);
        kafkaProducer.close();
    }
}
