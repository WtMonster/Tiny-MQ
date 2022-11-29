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
    public static void main(String[] args) throws InterruptedException {
        MqProducer mqProducer = new MqProducer();
        mqProducer.start();
        String message = "HELLO MQ!";
        MqMessage mqMessage = new MqMessage();
        mqMessage.setTopic("TOPIC");
        mqMessage.setTags(Arrays.asList("TAGA", "TAGB"));
        mqMessage.setPayload(message);

        SendResult sendResult = mqProducer.send(mqMessage);
        mqProducer.send(mqMessage);
        mqProducer.send(mqMessage);

        System.out.println(JSON.toJSON(sendResult));

        System.exit(0);
    }
}
