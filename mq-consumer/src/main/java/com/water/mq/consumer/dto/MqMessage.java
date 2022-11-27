package com.water.mq.consumer.dto;

import java.util.List;

public class MqMessage {
    /**
     * 标题名称
     */
    private String topic;

    /**
     * 标签
     */
    private List<String> tags;

    /**
     * 内容
     */
    private byte[] payload;

    /**
     * 业务标识
     */
    private String bizKey;

    /**
     * 负载分片标识
     */
    private String shardingKey;

    // getter&setter&toString

}
