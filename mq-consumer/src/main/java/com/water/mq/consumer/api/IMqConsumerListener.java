package com.water.mq.consumer.api;

import com.water.mq.common.dto.req.MqMessage;
import com.water.mq.common.resp.ConsumerStatus;

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
