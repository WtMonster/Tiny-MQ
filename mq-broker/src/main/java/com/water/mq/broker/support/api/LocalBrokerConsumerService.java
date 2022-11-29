package com.water.mq.broker.support.api;

import com.github.houbb.heaven.util.util.CollectionUtil;
import com.water.mq.broker.api.IBrokerConsumerService;
import com.water.mq.broker.dto.BrokerServiceEntryChannel;
import com.water.mq.broker.dto.ServiceEntry;
import com.water.mq.broker.dto.consumer.ConsumerSubscribeBo;
import com.water.mq.broker.dto.consumer.ConsumerSubscribeReq;
import com.water.mq.broker.dto.consumer.ConsumerUnSubscribeReq;
import com.water.mq.broker.utils.InnerChannelUtils;
import com.water.mq.common.dto.req.MqMessage;
import com.water.mq.common.dto.resp.MqCommonResp;
import com.water.mq.common.resp.MqCommonRespCode;
import com.water.mq.common.util.ChannelUtil;
import com.water.mq.common.util.RandomUtils;
import com.water.mq.common.util.RegexUtils;
import io.netty.channel.Channel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * @author binbin.hou
 * @since 1.0.0
 */
public class LocalBrokerConsumerService implements IBrokerConsumerService {

    private final Map<String, BrokerServiceEntryChannel> registerMap = new ConcurrentHashMap<>();

    /**
     * 订阅集合
     * key: topicName
     * value: 对应的订阅列表
     */
    private final Map<String, Set<ConsumerSubscribeBo>> subscribeMap = new ConcurrentHashMap<>();

    @Override
    public MqCommonResp register(ServiceEntry serviceEntry, Channel channel) {
        final String channelId = ChannelUtil.getChannelId(channel);
        BrokerServiceEntryChannel entryChannel = InnerChannelUtils.buildEntryChannel(serviceEntry, channel);
        registerMap.put(channelId, entryChannel);

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
    public MqCommonResp subscribe(ConsumerSubscribeReq serviceEntry, Channel clientChannel) {
        final String channelId = ChannelUtil.getChannelId(clientChannel);
        final String topicName = serviceEntry.getTopicName();
        // 组装消费者
        ConsumerSubscribeBo subscribeBo = new ConsumerSubscribeBo();
        subscribeBo.setChannelId(channelId);
        subscribeBo.setGroupName(serviceEntry.getGroupName());
        subscribeBo.setTopicName(topicName);
        subscribeBo.setTagRegex(serviceEntry.getTagRegex());

        Set<ConsumerSubscribeBo> set = subscribeMap.get(topicName);
        if(set == null) {
            set = new HashSet<>();
            subscribeMap.put(topicName, set);
        }
        set.add(subscribeBo);

        MqCommonResp resp = new MqCommonResp();
        resp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        return resp;
    }

    @Override
    public MqCommonResp unSubscribe(ConsumerUnSubscribeReq serviceEntry, Channel clientChannel) {
        final String channelId = ChannelUtil.getChannelId(clientChannel);
        final String topicName = serviceEntry.getTopicName();

        ConsumerSubscribeBo subscribeBo = new ConsumerSubscribeBo();
        subscribeBo.setChannelId(channelId);
        subscribeBo.setGroupName(serviceEntry.getGroupName());
        subscribeBo.setTopicName(topicName);
        subscribeBo.setTagRegex(serviceEntry.getTagRegex());

        // 集合
        Set<ConsumerSubscribeBo> set = subscribeMap.get(topicName);
        if(CollectionUtil.isNotEmpty(set)) {
            set.remove(subscribeBo);
        }

        MqCommonResp resp = new MqCommonResp();
        resp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        return resp;
    }

    @Override
    public List<Channel> getSubscribeList(MqMessage mqMessage) {
        final String topicName = mqMessage.getTopic();
        Set<ConsumerSubscribeBo> set = subscribeMap.get(topicName);
        if(CollectionUtil.isEmpty(set)) {
            return Collections.emptyList();
        }

        //2. 获取匹配的 tag 列表
        final List<String> tagNameList = mqMessage.getTags();

        Map<String, List<ConsumerSubscribeBo>> groupMap = new HashMap<>();
        for(ConsumerSubscribeBo bo : set) {
            String tagRegex = bo.getTagRegex();

            if(hasMatch(tagNameList, tagRegex)) {
                //TODO: 这种设置模式，统一添加处理
                String groupName = bo.getGroupName();
                List<ConsumerSubscribeBo> list = groupMap.get(groupName);
                if(list == null) {
                    list = new ArrayList<>();
                }
                list.add(bo);

                groupMap.put(groupName, list);
            }
        }

        //3. 按照 groupName 分组之后，每一组只随机返回一个。最好应该调整为以 shardingkey 选择
        final String shardingKey = mqMessage.getShardingKey();
        List<Channel> channelList = new ArrayList<>();

        for(Map.Entry<String, List<ConsumerSubscribeBo>> entry : groupMap.entrySet()) {
            List<ConsumerSubscribeBo> list = entry.getValue();

            ConsumerSubscribeBo bo = RandomUtils.random(list, shardingKey);
            BrokerServiceEntryChannel entryChannel = registerMap.get(bo.getChannelId());
            channelList.add(entryChannel.getChannel());
        }

        return channelList;
    }

    private boolean hasMatch(List<String> tagNameList,
                             String tagRegex) {
        if(CollectionUtil.isEmpty(tagNameList)) {
            return false;
        }

        Pattern pattern = Pattern.compile(tagRegex);

        for(String tagName : tagNameList) {
            if(RegexUtils.match(pattern, tagName)) {
                return true;
            }
        }

        return false;
    }

}
