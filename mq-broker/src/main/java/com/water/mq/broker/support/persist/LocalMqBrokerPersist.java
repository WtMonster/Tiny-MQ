package com.water.mq.broker.support.persist;

import com.alibaba.fastjson.JSON;
import com.github.houbb.heaven.util.util.CollectionUtil;
import com.github.houbb.log.integration.core.Log;
import com.github.houbb.log.integration.core.LogFactory;
import com.water.mq.broker.constant.MessageStatusConst;
import com.water.mq.broker.dto.persist.MqMessagePersistPut;
import com.water.mq.broker.utils.InnerRegexUtils;
import com.water.mq.common.dto.req.MqConsumerPullReq;
import com.water.mq.common.dto.req.MqMessage;
import com.water.mq.common.dto.resp.MqCommonResp;
import com.water.mq.common.dto.resp.MqConsumerPullResp;
import com.water.mq.common.resp.MqCommonRespCode;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地持久化策略
 *
 * @author binbin.hou
 * @since 1.0.0
 */
public class LocalMqBrokerPersist implements IMqBrokerPersist {

    private static final Log log = LogFactory.getLog(LocalMqBrokerPersist.class);

    /**
     * 队列
     * ps: 这里只是简化实现，暂时不考虑并发等问题。
     */
    private final Map<String, List<MqMessagePersistPut>> map = new ConcurrentHashMap<>();

    //1. 接收
    //2. 持久化
    //3. 通知消费
    @Override
    public synchronized MqCommonResp put(MqMessagePersistPut put) {
        log.info("put elem: {}", JSON.toJSON(put));

        MqMessage mqMessage = put.getMqMessage();
        // TODO: 这里final的意义是什么？
        final String topic = mqMessage.getTopic();

        List<MqMessagePersistPut> list = map.get(topic);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(put);
        // TODO: 这里似乎可以改写成如下代码,有时可以减少一次put操作
        /**
         * if (list == null) {
         *     list = new ArrayList<>();
         *     map.put(topic, list);
         * }
         * list.add(put);
         */
        map.put(topic, list);

        MqCommonResp commonResp = new MqCommonResp();
        commonResp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }

    @Override
    public MqCommonResp updateStatus(String messageId, String status) {
        // 这里性能比较差，所以不可以用于生产。仅作为测试验证
        // TODO: 改进复杂度
        for (List<MqMessagePersistPut> list : map.values()) {
            for (MqMessagePersistPut put : list) {
                MqMessage mqMessage = put.getMqMessage();
                if (mqMessage.getTraceId().equals(messageId)) {
                    put.setMessageStatus(status);

                    break;
                }
            }
        }

        MqCommonResp commonResp = new MqCommonResp();
        commonResp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }

    @Override
    public MqConsumerPullResp pull(MqConsumerPullReq pullReq, Channel channel) {
        //1. 拉取匹配的信息
        //2. 状态更新为代理中
        //3. 如何更新对应的消费状态呢？

        // 获取状态为 W 的订单
        final int fetchSize = pullReq.getSize();
        final String topic = pullReq.getTopicName();
        final String tagRegex = pullReq.getTagRegex();

        List<MqMessage> resultList = new ArrayList<>(fetchSize);
        List<MqMessagePersistPut> putList = map.get(topic);
        // 性能比较差
        if(CollectionUtil.isNotEmpty(putList)) {
            for(MqMessagePersistPut put : putList) {
                final String status = put.getMessageStatus();
                if(!MessageStatusConst.WAIT_CONSUMER.equals(status)) {
                    continue;
                }

                final MqMessage mqMessage = put.getMqMessage();
                List<String> tagList = mqMessage.getTags();
                if(InnerRegexUtils.hasMatch(tagList, tagRegex)) {
                    // 设置为处理中
                    // TODO： 消息的最终状态什么时候更新呢？
                    // 可以给 broker 一个 ACK
                    put.setMessageStatus(MessageStatusConst.PROCESS_CONSUMER);
                    resultList.add(mqMessage);
                }

                if(resultList.size() >= fetchSize) {
                    break;
                }
            }
        }

        MqConsumerPullResp resp = new MqConsumerPullResp();
        resp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        resp.setList(resultList);
        return resp;
    }

}
