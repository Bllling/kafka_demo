package com.lin.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class UserConsumer {
    private static final String brokerList = "192.168.18.128:9092";
    private static final String topic = "lin";
    private static final String groupId = "group.demo";
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置key序列化器
        //properties.put("key.deserializer",
        //        "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置值序列化
        //properties.put("value.deserializer",
        //        "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class.getName());

        // 设置消费组
        //properties.put("group.id", groupId);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //设置集群地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        KafkaConsumer<String,User> kafkaConsumer = new KafkaConsumer<String, User>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        while (true){
            ConsumerRecords<String,User> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String,User> record:records){
                System.out.println(record.value());
            }
        }
    }
}
