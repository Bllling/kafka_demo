package com.lin.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class UserSerializer implements Serializer<User> {
    ObjectMapper mapper;
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String s, User user) {
        try {
            return mapper.writeValueAsString(user).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        //if (user == null){
        //    return null;
        //}
        //byte[] targetName,targetSex;
        //
        //
        //try {
        //    if (user.getName() != null){
        //        targetName = user.getName().getBytes("UTF-8");
        //    }else{
        //        targetName = new byte[0];
        //    }
        //    if (user.getSex() != null){
        //        targetSex = user.getSex().getBytes("UTF-8");
        //    }else{
        //        targetSex = new byte[0];
        //    }
        //    ByteBuffer buffer = ByteBuffer.allocate(4+4+4+targetName.length+targetSex.length);
        //    buffer.putInt(user.getId());
        //    buffer.putInt(targetName.length);
        //    buffer.put(targetName);
        //    buffer.putInt(targetSex.length);
        //    buffer.put(targetSex);
        //    return  buffer.array();
        //} catch (UnsupportedEncodingException e) {
        //    e.printStackTrace();
        //}
        return null;
    }

    @Override
    public void close() {

    }
}
