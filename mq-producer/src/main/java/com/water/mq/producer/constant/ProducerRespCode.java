package com.water.mq.producer.constant;

import com.github.houbb.heaven.response.respcode.RespCode;

/**
 * @author WtMonster
 * @date 2022/11/27 16:07
 */
public enum ProducerRespCode implements RespCode {

    RPC_INIT_FAILED("P00001", "生产者启动失败");

    private final String code;
    private final String msg;

    ProducerRespCode(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMsg() {
        return msg;
    }
}
