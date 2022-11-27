package com.water.mq.consumer.api;

import com.water.mq.consumer.constant.ConsumerStatus;
import com.water.mq.consumer.dto.MqMessage;

public interface IMqConsumerListener {
    /**
     * 消费
     * @param mqMessage 消息体
     * @param context 上下文
     * @return 结果
     */
    ConsumerStatus consumer(final MqMessage mqMessage,
                            final IMqConsumerListenerContext context);

}
