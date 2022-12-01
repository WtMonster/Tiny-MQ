/*
 * Copyright (c)  2019. houbinbin Inc.
 * rpc All rights reserved.
 */

package com.water.mq.broker.support.api;


import com.github.houbb.log.integration.core.Log;
import com.github.houbb.log.integration.core.LogFactory;
import com.water.mq.broker.api.IBrokerProducerService;
import com.water.mq.broker.dto.BrokerServiceEntryChannel;
import com.water.mq.broker.dto.ServiceEntry;
import com.water.mq.broker.utils.InnerChannelUtils;
import com.water.mq.common.dto.resp.MqCommonResp;
import com.water.mq.common.resp.MqCommonRespCode;
import com.water.mq.common.util.ChannelUtil;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p> 生产者注册服务类 </p>
 *
 * <pre> Created: 2019/10/23 9:08 下午  </pre>
 * <pre> Project: rpc  </pre>
 *
 * @author houbinbin
 * @since 0.0.3
 */
public class LocalBrokerProducerService implements IBrokerProducerService {

    private static final Log log = LogFactory.getLog(LocalBrokerProducerService.class);

    // 存储channelId和连接信息的对应关系
    private final Map<String, BrokerServiceEntryChannel> registerMap = new ConcurrentHashMap<>();

    @Override
    public MqCommonResp register(ServiceEntry serviceEntry, Channel channel) {
        // 构建注册信息，并注册
        final String channelId = ChannelUtil.getChannelId(channel);
        BrokerServiceEntryChannel entryChannel = InnerChannelUtils.buildEntryChannel(serviceEntry, channel);
        registerMap.put(channelId, entryChannel);

        // 返回注册成功的消息
        MqCommonResp resp = new MqCommonResp();
        resp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        return resp;
    }

    @Override
    public MqCommonResp unRegister(ServiceEntry serviceEntry, Channel channel) {
        final String channelId = ChannelUtil.getChannelId(channel);
        registerMap.remove(channelId);

        MqCommonResp resp = new MqCommonResp();
        resp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        return resp;
    }

    @Override
    public ServiceEntry getServiceEntry(String channelId) {
        return registerMap.get(channelId);
    }

}
