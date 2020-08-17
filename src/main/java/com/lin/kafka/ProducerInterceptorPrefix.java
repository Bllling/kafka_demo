package com.lin.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截器
 */
public class ProducerInterceptorPrefix  implements ProducerInterceptor<String,User> {
    private volatile long sendSuccess = 0;
    private volatile long sendFail = 0;

    /**
     * 对消息的过滤，操作在此方法
     * 对user添加一个新的属性值
     * @param producerRecord
     * @return
     */
    @Override
    public ProducerRecord<String, User> onSend(ProducerRecord<String, User> producerRecord) {
        User user = producerRecord.value();
        user.setUpdate(1);
        return new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(),
                producerRecord.timestamp(), producerRecord.key(), user, producerRecord.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e == null){
            sendSuccess++;
        }else {
            sendFail++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess/(sendFail+sendSuccess);
        System.out.println("成功率为"+successRatio*100);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
