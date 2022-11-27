package consumer;

import com.water.mq.consumer.core.MqConsumerPush;

/**
 * @author WtMonster
 * @date 2022/11/27 16:07
 */
public class ConsumerMain {

    //1. 首先启动消费者，然后启动生产者。
    public static void main(String[] args) {
        MqConsumerPush mqConsumerPush = new MqConsumerPush();
        mqConsumerPush.start();
    }

}
