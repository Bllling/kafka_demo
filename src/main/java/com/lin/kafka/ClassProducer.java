package com.lin.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 自定义序列化器
 */
public class ClassProducer {
    private static final String brokerList = "192.168.18.128:9092";
    private static final String topic = "lin";
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置key的序列化器
        //properties.put("key.serializer",
        //        "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置重复次数
        properties.put(ProducerConfig.RETRIES_CONFIG,10);
        // 设置值的序列化
        //properties.put("value.serializer",
        //        "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ClassSerializer.class.getName());
        //设置集群地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        KafkaProducer<String,User> kafkaProducer = new KafkaProducer<String, User>(properties);
        User user = new User();
        user.setId(1);
        user.setName("娅娅");
        user.setSex("女");
        ProducerRecord<String,User> producerRecord = new ProducerRecord<>(topic, "kafka_demo", user);
        try {
            // 同步发送
            //  Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
            //  RecordMetadata recordMetadata = send.get();
            //  System.out.println("topic:"+recordMetadata.topic());
            //  System.out.println("partition:"+recordMetadata.partition());
            //  System.out.println("offset:"+recordMetadata.offset());

            // 异步发送
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        System.out.println("topic:"+recordMetadata.topic());
                        System.out.println("partition:"+recordMetadata.partition());
                        System.out.println("offset:"+recordMetadata.offset());
                    }
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
        kafkaProducer.close();
    }
}
