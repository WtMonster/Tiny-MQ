package com.water.mq.broker.dto;

import com.water.mq.common.dto.req.MqCommonReq;

/**
 * @author binbin.hou
 * @since 1.0.0
 */
public class BrokerRegisterReq extends MqCommonReq {

    /**
     * 服务信息
     */
    private ServiceEntry serviceEntry;

    public ServiceEntry getServiceEntry() {
        return serviceEntry;
    }

    public void setServiceEntry(ServiceEntry serviceEntry) {
        this.serviceEntry = serviceEntry;
    }
}
