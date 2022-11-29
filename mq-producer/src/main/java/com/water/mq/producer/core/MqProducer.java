package com.water.mq.producer.core;

import com.alibaba.fastjson.JSON;
import com.github.houbb.heaven.util.common.ArgUtil;
import com.github.houbb.heaven.util.util.DateUtil;
import com.github.houbb.id.core.util.IdHelper;
import com.github.houbb.log.integration.core.Log;
import com.github.houbb.log.integration.core.LogFactory;
import com.water.mq.broker.dto.BrokerRegisterReq;
import com.water.mq.broker.dto.ServiceEntry;
import com.water.mq.common.constant.MethodType;
import com.water.mq.common.dto.req.MqCommonReq;
import com.water.mq.common.dto.req.MqMessage;
import com.water.mq.common.dto.resp.MqCommonResp;
import com.water.mq.common.resp.MqCommonRespCode;
import com.water.mq.common.resp.MqException;
import com.water.mq.common.rpc.RpcChannelFuture;
import com.water.mq.common.rpc.RpcMessageDto;
import com.water.mq.common.support.invoke.IInvokeService;
import com.water.mq.common.support.invoke.impl.InvokeService;
import com.water.mq.common.util.ChannelFutureUtils;
import com.water.mq.common.util.ChannelUtil;
import com.water.mq.common.util.DelimiterUtil;
import com.water.mq.common.util.RandomUtils;
import com.water.mq.producer.api.IMqProducer;
import com.water.mq.producer.constant.ProducerConst;
import com.water.mq.producer.constant.ProducerRespCode;
import com.water.mq.producer.constant.SendStatus;
import com.water.mq.producer.dto.SendResult;
import com.water.mq.producer.handler.MqProducerHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;

import java.util.List;

/**
 * @author WtMonster
 * @date 2022/11/27 16:07
 */
public class MqProducer extends Thread implements IMqProducer {

    private static final Log log = LogFactory.getLog(MqProducer.class);

    /**
     * 分组名称
     */
    private final String groupName;

    /**
     * 端口号
     */
    private final int port;

    /**
     * 中间人地址
     */
    private String brokerAddress  = "127.0.0.1:9999";

    /**
     * channel 信息
     * @since 0.0.2
     */
    private ChannelFuture channelFuture;

    /**
     * 客户端处理 handler
     * @since 0.0.2
     */
    private ChannelHandler channelHandler;

    /**
     * 调用管理服务
     * @since 0.0.2
     */
    private final IInvokeService invokeService = new InvokeService();

    /**
     * 获取响应超时时间
     * @since 0.0.2
     */
    private long respTimeoutMills = 5000;

    /**
     * 可用标识
     * @since 0.0.2
     */
    private volatile boolean enableFlag = false;

    /**
     * 粘包处理分隔符
     * @since 1.0.0
     */
    private String delimiter = DelimiterUtil.DELIMITER;


    /**
     * 请求列表
     * @since 0.0.3
     */
    private List<RpcChannelFuture> channelFutureList;


    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public MqProducer(String groupName, int port) {
        this.groupName = groupName;
        this.port = port;
    }

    public MqProducer(String groupName) {
        this(groupName, ProducerConst.DEFAULT_PORT);
    }

    public MqProducer() {
        this(ProducerConst.DEFAULT_GROUP_NAME, ProducerConst.DEFAULT_PORT);
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    public void setRespTimeoutMills(long respTimeoutMills) {
        this.respTimeoutMills = respTimeoutMills;
    }

    public boolean isEnableFlag() {
        return enableFlag;
    }

    private ChannelHandler initChannelHandler() {
        final ByteBuf delimiterBuf = DelimiterUtil.getByteBuf(delimiter);

        final MqProducerHandler mqProducerHandler = new MqProducerHandler();
        mqProducerHandler.setInvokeService(invokeService);

        // TODO: 那这里用不用改呢
        // handler 实际上会被多次调用，如果不是 @Shareable，应该每次都重新创建。
        ChannelHandler handler = new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch.pipeline()
                        .addLast(new DelimiterBasedFrameDecoder(DelimiterUtil.LENGTH, delimiterBuf))
                        .addLast(mqProducerHandler);
            }
        };

        this.channelHandler = handler;

        return handler;
    }

    /**
     * 参数校验
     */
    private void paramCheck() {
        ArgUtil.notEmpty(brokerAddress, "brokerAddress");
    }

    @Override
    public synchronized void run() {
        this.paramCheck();

        // 启动服务端
        log.info("MQ 生产者开始启动客户端 GROUP: {}, PORT: {}, brokerAddress: {}",
                groupName, port, brokerAddress);

        try {
            // channel handler
            ChannelHandler channelHandler = this.initChannelHandler();

            //channel future
            this.channelFutureList = ChannelFutureUtils.initChannelFutureList(brokerAddress, channelHandler);

            // register to broker
            this.registerToBroker();

            // 标识为可用
            enableFlag = true;
            log.info("MQ 生产者启动完成");
        } catch (Exception e) {
            log.error("MQ 生产者启动遇到异常", e);
            throw new MqException(ProducerRespCode.RPC_INIT_FAILED);
        }
    }

    /**
     * 注册到所有的服务端
     * @since 0.0.3
     */
    private void registerToBroker() {
        for(RpcChannelFuture channelFuture : this.channelFutureList) {
            ServiceEntry serviceEntry = new ServiceEntry();
            serviceEntry.setGroupName(groupName);
            serviceEntry.setAddress(channelFuture.getAddress());
            serviceEntry.setPort(channelFuture.getPort());
            serviceEntry.setWeight(channelFuture.getWeight());

            BrokerRegisterReq brokerRegisterReq = new BrokerRegisterReq();
            brokerRegisterReq.setServiceEntry(serviceEntry);
            brokerRegisterReq.setMethodType(MethodType.P_REGISTER);
            brokerRegisterReq.setTraceId(IdHelper.uuid32());

            log.info("[Register] 开始注册到 broker：{}", JSON.toJSON(brokerRegisterReq));
            final Channel channel = channelFuture.getChannelFuture().channel();
            MqCommonResp resp = callServer(channel, brokerRegisterReq, MqCommonResp.class);
            log.info("[Register] 完成注册到 broker：{}", JSON.toJSON(resp));
        }
    }

    /***
     * @description: 发送消息
     * @param: mqMessage
     * @return: com.water.mq.producer.dto.SendResult
     */
    @Override
    public SendResult send(MqMessage mqMessage) {
        // 生成id随机，类型为请求的消息
        String messageId = IdHelper.uuid32();
        mqMessage.setTraceId(messageId);
        mqMessage.setMethodType(MethodType.P_SEND_MSG);

        Channel channel = getChannel(mqMessage.getShardingKey());
        MqCommonResp resp = callServer(channel, mqMessage, MqCommonResp.class);
        if(MqCommonRespCode.SUCCESS.getCode().equals(resp.getRespCode())) {
            return SendResult.of(messageId, SendStatus.SUCCESS);
        }

        return SendResult.of(messageId, SendStatus.FAILED);
    }

    /***
     * @description: 不关心请求发送的结果，直接认为成功
     * @param: mqMessage
     * @return: com.water.mq.producer.dto.SendResult
     */
    @Override
    public SendResult sendOneWay(MqMessage mqMessage) {
        String messageId = IdHelper.uuid32();
        mqMessage.setTraceId(messageId);
        mqMessage.setMethodType(MethodType.P_SEND_MSG);

        Channel channel = getChannel(mqMessage.getShardingKey());
        this.callServer(channel, mqMessage, null);

        return SendResult.of(messageId, SendStatus.SUCCESS);
    }

    /**
     * 调用服务端
     * @param channel 调用通道
     * @param commonReq 通用请求
     * @param respClass 类
     * @param <T> 泛型
     * @param <R> 结果
     * @return 结果
     * @since 1.0.0
     */
    private <T extends MqCommonReq, R extends MqCommonResp> R callServer(Channel channel,
                                                                         T commonReq,
                                                                         Class<R> respClass) {
        final String traceId = commonReq.getTraceId();
        final long requestTime = System.currentTimeMillis();

        RpcMessageDto rpcMessageDto = new RpcMessageDto();
        rpcMessageDto.setTraceId(traceId);
        rpcMessageDto.setRequestTime(requestTime);
        rpcMessageDto.setJson(JSON.toJSONString(commonReq));
        rpcMessageDto.setMethodType(commonReq.getMethodType());
        rpcMessageDto.setRequest(true);

        // 添加调用服务
        invokeService.addRequest(traceId, respTimeoutMills);

        // 遍历 channel
        // 关闭当前线程，以获取对应的信息
        // 使用序列化的方式
        ByteBuf byteBuf = DelimiterUtil.getMessageDelimiterBuffer(rpcMessageDto);

        //负载均衡获取 channel
        channel.writeAndFlush(byteBuf);

        String channelId = ChannelUtil.getChannelId(channel);
        log.debug("[Client] channelId {} 发送消息 {}", channelId, JSON.toJSON(rpcMessageDto));
//        channel.closeFuture().syncUninterruptibly();

        if (respClass == null) {
            log.debug("[Client] 当前消息为 one-way 消息，忽略响应");
            return null;
        } else {
            //channelHandler 中获取对应的响应
            RpcMessageDto messageDto = invokeService.getResponse(traceId);
            if (MqCommonRespCode.TIMEOUT.getCode().equals(messageDto.getRespCode())) {
                throw new MqException(MqCommonRespCode.TIMEOUT);
            }

            String respJson = messageDto.getJson();
            return JSON.parseObject(respJson, respClass);
        }
    }


    /**
     * 获取请求通道
     * @param key 标识
     * @return 结果
     */
    private Channel getChannel(String key) {
        // 等待启动完成
        while (!enableFlag) {
            log.debug("等待初始化完成...");
            DateUtil.sleep(100);
        }

        RpcChannelFuture rpcChannelFuture = RandomUtils.random(channelFutureList, key);
        return rpcChannelFuture.getChannelFuture().channel();
    }

}
