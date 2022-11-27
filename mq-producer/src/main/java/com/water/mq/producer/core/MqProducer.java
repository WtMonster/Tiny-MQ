package com.water.mq.producer.core;

import com.github.houbb.log.integration.core.Log;
import com.github.houbb.log.integration.core.LogFactory;
import com.water.mq.common.mq.common.dto.MqMessage;
import com.water.mq.common.mq.common.exception.MqException;
import com.water.mq.producer.api.IMqProducer;
import com.water.mq.producer.constant.ProducerConst;
import com.water.mq.producer.constant.ProducerRespCode;
import com.water.mq.producer.dto.SendResult;
import com.water.mq.producer.handler.MqProducerHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

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
    private String brokerAddress  = "";

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

    @Override
    public void run() {
        // 启动服务端
        log.info("MQ 消费者开始启动客户端 GROUP: {}, PORT: {}, brokerAddress: {}",
                groupName, port, brokerAddress);

        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            ChannelFuture channelFuture = bootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<Channel>(){
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new LoggingHandler(LogLevel.INFO))
                                    .addLast(new MqProducerHandler());
                        }
                    })
                    .connect("localhost", port)
                    .syncUninterruptibly();

            log.info("MQ 消费者启动客户端完成，监听端口：" + port);
            channelFuture.channel().closeFuture().syncUninterruptibly();
            log.info("MQ 消费者开始客户端已关闭");
        } catch (Exception e) {
            log.error("MQ 消费者启动遇到异常", e);
            throw new MqException(ProducerRespCode.RPC_INIT_FAILED);
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    public SendResult send(MqMessage mqMessage) {
        return null;
    }

    @Override
    public SendResult sendOneWay(MqMessage mqMessage) {
        return null;
    }

}
