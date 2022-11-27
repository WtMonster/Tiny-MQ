package producer;

import com.alibaba.fastjson.JSON;
import com.github.houbb.heaven.util.util.DateUtil;
import com.water.mq.common.dto.req.MqMessage;
import com.water.mq.producer.core.MqProducer;
import com.water.mq.producer.dto.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author WtMonster
 * @date 2022/11/27 16:14
 */
public class ProducerMain {
    public static void main(String[] args) {
        MqProducer mqProducer = new MqProducer();
        mqProducer.start();

        //等待启动完成
        while (!mqProducer.isEnableFlag()) {
            System.out.println("等待初始化完成...");
            DateUtil.sleep(100);
        }

        String message = "HELLO MQ!";
        MqMessage mqMessage = new MqMessage();
        mqMessage.setTopic("TOPIC");
        mqMessage.setTags(Arrays.asList("TAGA", "TAGB"));
        mqMessage.setPayload(message.getBytes(StandardCharsets.UTF_8));

        SendResult sendResult = mqProducer.send(mqMessage);
        System.out.println(JSON.toJSON(sendResult));
    }
}
