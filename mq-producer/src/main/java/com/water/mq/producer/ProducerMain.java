package com.water.mq.producer;

import com.alibaba.fastjson.JSON;
import com.water.mq.common.dto.req.MqMessage;
import com.water.mq.producer.core.MqProducer;
import com.water.mq.producer.dto.SendResult;
import com.water.mq.producer.prometheus.PrometheusConfig;

import java.util.Arrays;
import java.util.UUID;

/**
 * @author WtMonster
 * @date 2022/11/27 16:14
 */
public class ProducerMain {

    public static void main(String[] args) throws InterruptedException {
        PrometheusConfig.init();
        MqProducer mqProducer = new MqProducer();
        mqProducer.start();
        UUID uuid = UUID.randomUUID();
        String message = "HELLO MQ! uuid:" + uuid;
        MqMessage mqMessage = new MqMessage();
        mqMessage.setTopic("TOPIC");
        
        mqMessage.setTags(Arrays.asList("TAGA", "TAGB"));
        mqMessage.setPayload(message);

        while (true) {
            SendResult sendResult = mqProducer.send(mqMessage);
            System.out.println(JSON.toJSON(sendResult));
            int pauseTime = ((int) (Math.random() * 10) + 1) * 1000;
            System.out.println(pauseTime);
            Thread.sleep(pauseTime);

        }

    }
}
