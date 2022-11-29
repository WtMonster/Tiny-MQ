package broker;

import com.water.mq.broker.core.MqBroker;

/**
 * @author WtMonster
 * @date 2022/11/29 1:14
 */
public class BrokerMain {
    public static void main(String[] args) {
        MqBroker broker = new MqBroker();
        broker.start();
    }
}
