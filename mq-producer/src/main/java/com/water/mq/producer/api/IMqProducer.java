package com.water.mq.producer.api;

import com.water.mq.common.dto.req.MqMessage;
import com.water.mq.producer.dto.SendResult;

/**
 * @author WtMonster
 * @date 2022/11/27 16:05
 */
public interface IMqProducer {

    /**
     * 同步发送消息
     * @param mqMessage 消息类型
     * @return 结果
     */
    SendResult send(final MqMessage mqMessage);

    /**
     * 单向发送消息
     * @param mqMessage 消息类型
     * @return 结果
     */
    SendResult sendOneWay(final MqMessage mqMessage);

}
