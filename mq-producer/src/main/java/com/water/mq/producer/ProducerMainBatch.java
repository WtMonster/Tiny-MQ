package com.water.mq.producer;

import com.alibaba.fastjson.JSON;
import com.water.mq.common.dto.req.MqMessage;
import com.water.mq.producer.core.MqProducer;
import com.water.mq.producer.dto.SendBatchResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author binbin.hou
 * @since 1.0.0
 */
public class ProducerMainBatch {

    public static void main(String[] args) {
        MqProducer mqProducer = new MqProducer();
        mqProducer.start();

        List<MqMessage> mqMessageList = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            MqMessage mqMessage = buildMessage(i);
            mqMessageList.add(mqMessage);
        }

        SendBatchResult sendResult = mqProducer.sendBatch(mqMessageList);
        System.out.println(JSON.toJSON(sendResult));
    }

    private static MqMessage buildMessage(int i) {
        String message = "HELLO MQ!" + i;
        MqMessage mqMessage = new MqMessage();
        mqMessage.setTopic("TOPIC");
        mqMessage.setTags(Arrays.asList("TAGA", "TAGB"));
        mqMessage.setPayload(message);

        return mqMessage;
    }

}
