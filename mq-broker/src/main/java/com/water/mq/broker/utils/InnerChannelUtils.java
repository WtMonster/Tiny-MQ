package com.water.mq.broker.utils;

import com.water.mq.broker.dto.BrokerServiceEntryChannel;
import com.water.mq.broker.dto.ServiceEntry;
import io.netty.channel.Channel;

/**
 * @author binbin.hou
 * @since 1.0.0
 */
public class InnerChannelUtils {

    private InnerChannelUtils(){}

    public static BrokerServiceEntryChannel buildEntryChannel(ServiceEntry serviceEntry,
                                                              Channel channel) {

        // TODO: 这里可以直接用工具类拷贝属性，增加可读性
        BrokerServiceEntryChannel result = new BrokerServiceEntryChannel();
        result.setChannel(channel);
        result.setGroupName(serviceEntry.getGroupName());
        result.setAddress(serviceEntry.getAddress());
        result.setPort(serviceEntry.getPort());
        result.setWeight(serviceEntry.getWeight());
        return result;
    }

}
