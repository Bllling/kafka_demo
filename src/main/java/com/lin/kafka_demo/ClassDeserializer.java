package com.lin.kafka_demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.ParameterizedType;
import java.util.Map;

/**
 * 自定义反序列化器
 */
public class ClassDeserializer<T> implements Deserializer<T> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public T deserialize(String s, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        T t = null;
        try {
            t = mapper.readValue(data,  (Class < T > ) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[ 0 ]);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return t;
    }

    @Override
    public void close() {

    }
}
