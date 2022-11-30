package com.water.mq.common.util;

import com.github.houbb.heaven.annotation.CommonEager;
import com.github.houbb.heaven.util.lang.StringUtil;
import com.github.houbb.heaven.util.util.CollectionUtil;
import com.github.houbb.heaven.util.util.RandomUtil;
import com.github.houbb.load.balance.api.ILoadBalance;
import com.github.houbb.load.balance.api.impl.LoadBalanceContext;
import com.github.houbb.load.balance.support.server.IServer;

import java.util.List;
import java.util.Objects;

/**
 * @author binbin.hou
 * @since 1.0.0
 */
@CommonEager
public class RandomUtils {

    /**
     * 负载均衡
     *
     * @param list 列表
     * @param key 分片键
     * @return 结果
     * @since 0.0.7
     */
    public static <T extends IServer> T loadBalance(final ILoadBalance<T> loadBalance,
                                                    final List<T> list, String key) {
        if(CollectionUtil.isEmpty(list)) {
            return null;
        }

        // 如果没有负载分片标识，那就使用工具类根据负载轮询
        // TODO: 这里是策略模式，但是写死了是负载轮询，如果改成可配置应该会好一些
        if(StringUtil.isEmpty(key)) {
            LoadBalanceContext<T> loadBalanceContext = LoadBalanceContext.<T>newInstance()
                    .servers(list);
            return loadBalance.select(loadBalanceContext);
        }

        // 获取 code
        int hashCode = Objects.hash(key);
        int index = hashCode % list.size();
        return list.get(index);
    }

}
