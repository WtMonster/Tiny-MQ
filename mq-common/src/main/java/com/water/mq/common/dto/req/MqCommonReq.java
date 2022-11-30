package com.water.mq.common.dto.req;

import java.io.Serializable;

/**
 * TODO: 有必要用那么多不同的实现类吗？
 * @author binbin.hou
 * @since 1.0.0
 */
public class MqCommonReq implements Serializable {

    /**
     * 请求标识
     */
    private String traceId;

    /**
     * 方法类型
     */
    private String methodType;

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getMethodType() {
        return methodType;
    }

    public void setMethodType(String methodType) {
        this.methodType = methodType;
    }
}
