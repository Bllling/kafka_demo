package com.lin.kafka_demo;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义反序列化器
 */
public class UserDeserializer implements Deserializer<User> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public User deserialize(String s, byte[] data) {
        Integer id;
        int nameSize;
        String name;
        int sexSize;
        String sex;
        try {
            if (null == data){
                return null;
            }
            if (data.length < 12){
                throw  new SerializationException("接收的数据的大小比预期的要短");
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);

            id = Integer.valueOf(buffer.getInt());

            nameSize = buffer.getInt();
            byte[] nameBytes = new byte[nameSize];
            buffer.get(nameBytes);
            name = new String(nameBytes, "UTF-8");

            sexSize = buffer.getInt();
            byte[] sexBytes = new byte[sexSize];
            buffer.get(sexBytes);
            sex = new String(sexBytes, "UTF-8");

            User user = new User();
            user.setId(id);
            user.setSex(sex);
            user.setName(name);

            return user;
        } catch (Exception e){
            throw new SerializationException("反序列化出错" + e);
        }
    }

    @Override
    public void close() {

    }
}
