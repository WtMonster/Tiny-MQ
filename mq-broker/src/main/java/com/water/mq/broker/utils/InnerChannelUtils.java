package com.water.mq.broker.utils;

import com.water.mq.broker.dto.BrokerServiceEntryChannel;
import com.water.mq.broker.dto.ServiceEntry;
import com.water.mq.common.rpc.RpcChannelFuture;
import io.netty.channel.Channel;

/**
 * @author binbin.hou
 * @since 1.0.0
 */
public class InnerChannelUtils {

    private InnerChannelUtils(){}

    /**
     * 构建基本服务地址
     * @param rpcChannelFuture 信息
     * @return 结果
     * @since 0.0.5
     */
    public static ServiceEntry buildServiceEntry(RpcChannelFuture rpcChannelFuture) {
        ServiceEntry serviceEntry = new ServiceEntry();

        serviceEntry.setAddress(rpcChannelFuture.getAddress());
        serviceEntry.setPort(rpcChannelFuture.getPort());
        serviceEntry.setWeight(rpcChannelFuture.getWeight());
        return serviceEntry;
    }

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
