package com.water.mq.consumer;

import com.alibaba.fastjson.JSON;
import com.water.mq.common.dto.req.MqMessage;
import com.water.mq.common.resp.ConsumerStatus;
import com.water.mq.consumer.api.IMqConsumerListener;
import com.water.mq.consumer.api.IMqConsumerListenerContext;
import com.water.mq.consumer.core.MqConsumerPush;
import com.water.mq.consumer.prometheus.PrometheusConfig;

import java.io.IOException;

/**
 * @author WtMonster
 * @date 2022/11/27 16:07
 */
public class ConsumerMain {

    // 先启动消费者，然后启动生产者。
    public static void main(String[] args) throws IOException {
        PrometheusConfig.init();
        final MqConsumerPush mqConsumerPush = new MqConsumerPush();
        mqConsumerPush.start();

        mqConsumerPush.subscribe("TOPIC", "TAGA");
        mqConsumerPush.registerListener(new IMqConsumerListener() {
            @Override
            public ConsumerStatus consumer(MqMessage mqMessage, IMqConsumerListenerContext context) {
                System.out.println("---------- 自定义 " + JSON.toJSONString(mqMessage));
                return ConsumerStatus.SUCCESS;
            }
        });
    }

}
