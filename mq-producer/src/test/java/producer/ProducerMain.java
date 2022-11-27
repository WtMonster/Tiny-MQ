package producer;

import com.water.mq.producer.core.MqProducer;

/**
 * @author WtMonster
 * @date 2022/11/27 16:14
 */
public class ProducerMain {
    public static void main(String[] args) {
        MqProducer producer = new MqProducer();
        producer.run();
    }
}
