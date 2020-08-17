package com.lin.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class ClassSerializer<T> implements Serializer<T> {
    ObjectMapper mapper;
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String s, T t) {
        try {
            return mapper.writeValueAsString(t).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
