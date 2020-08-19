package com.lin.springboot.kafka.controller;


import com.lin.springboot.kafka.pojo.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping
public class KafkaController {
    @Autowired
    private KafkaTemplate template;
    private static final String topic = "lin";

    /**
     * 消息生产者,用exucuteInTransction进行事务控制
     * @param input
     * 浏览器输入http://localhost:8080/send/[要发送的数据]
     * @return
     */
    @GetMapping("/send/{input}")
    public String sendToKafka(@PathVariable String input){
        //第一次发送，没有事务控制,因为配置文件中定义了事务控制，这行没有事务控制，所以程序运行会报错
        //this.template.send(topic,input);

        // 事务的支持,根据事务的一致性,如果发送的是error，那33行发送的数据会回滚
        template.executeInTransaction(t->{
           //第一次发送
           t.send(topic,input);
           if ("error".equals(input)){
               throw new RuntimeException("input is error");
           }
           //第二次发送
           t.send(topic,input+"author lin");
           return true;
        });

        return  "Send Success "+input;
    }


    /**
     * 注解型事务控制,相比上面那个,这个会更好
     * @param input
     * @return
     * @Transactional(rollbackFor = RuntimeException.class) 当抛出异常时，回滚
     */
    @GetMapping("/send2/{input}")
    @Transactional(rollbackFor = Exception.class)
    public String sendToKafka2(@PathVariable String input){
        //第一次发送
        this.template.send(topic,input);
        if ("error".equals(input)){
            throw new RuntimeException("input is error");
        }
        //第二次发送
        this.template.send(topic,input+"author lin");
        return  "Send Success "+input;
    }



    /**
     * 消息消费者,一直监听消息
     * @param input
     */
    @KafkaListener(id = "",topics = topic,groupId = "group.demo")
    public void listener(String input){
        System.out.println(input);
    }
}
