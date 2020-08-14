package com.lin.kafka_demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private static final String brokerList = "192.168.18.128:9092";
    private static final String topic = "lin";
    private static final String groupId = "group.demo";
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置key序列化器
//        properties.put("key.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置值序列化
//        properties.put("value.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 设置消费组
//        properties.put("group.id", groupId);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //设置集群地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        while (true){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (records == null){
                continue;
            }
            for (ConsumerRecord<String,String> record:records){
                System.out.println(record.value());
            }
        }
    }
}
