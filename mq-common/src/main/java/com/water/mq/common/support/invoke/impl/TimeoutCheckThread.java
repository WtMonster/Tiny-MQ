package com.water.mq.common.support.invoke.impl;

import com.github.houbb.heaven.util.common.ArgUtil;
import com.water.mq.common.rpc.RpcMessageDto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 超时检测线程
 * @author binbin.hou
 * @since 0.0.2
 */
public class TimeoutCheckThread implements Runnable {

    // TODO: 这里有必要用ConcurrentHashMap吗

    /**
     * 请求信息
     * @since 0.0.2
     */
    private final ConcurrentHashMap<String, Long> requestMap;

    /**
     * 请求信息
     * @since 0.0.2
     */
    private final ConcurrentHashMap<String, RpcMessageDto> responseMap;

    /**
     * 新建
     * @param requestMap  请求 Map
     * @param responseMap 结果 map
     * @since 0.0.2
     */
    public TimeoutCheckThread(ConcurrentHashMap<String, Long> requestMap,
                              ConcurrentHashMap<String, RpcMessageDto> responseMap) {
        ArgUtil.notNull(requestMap, "requestMap");
        this.requestMap = requestMap;
        this.responseMap = responseMap;
    }


    //TODO: 这里用map遍历是不是可以改成优先队列
    //TODO: 可以看看其他RPC的超时控制实现；为什么不用自带的超时机制？
    @Override
    public void run() {
        for(Map.Entry<String, Long> entry : requestMap.entrySet()) {
            long expireTime = entry.getValue();
            long currentTime = System.currentTimeMillis();

            if(currentTime > expireTime) {
                final String key = entry.getKey();
                // 结果设置为超时，从请求 map 中移除
                responseMap.putIfAbsent(key, RpcMessageDto.timeout());
                requestMap.remove(key);
            }
        }
    }

}
