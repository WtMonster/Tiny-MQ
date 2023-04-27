package com.water.mq.consumer.support.listener;


import com.water.mq.common.dto.req.MqMessage;
import com.water.mq.common.resp.ConsumerStatus;
import com.water.mq.consumer.api.IMqConsumerListener;
import com.water.mq.consumer.api.IMqConsumerListenerContext;

/**
 * @author binbin.hou
 * @since 0.0.3
 */
public interface IMqListenerService {

    /**
     * 注册
     * @param listener 监听器
     * @since 0.0.3
     */
    void register(final IMqConsumerListener listener);

    /**
     * 消费消息
     * @param mqMessage 消息
     * @param context 上下文
     * @return 结果
     * @since 0.0.3
     */
    ConsumerStatus consumer(final MqMessage mqMessage,
                            final IMqConsumerListenerContext context);

}
