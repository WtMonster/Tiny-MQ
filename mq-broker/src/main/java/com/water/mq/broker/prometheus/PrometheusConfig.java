package com.water.mq.broker.prometheus;

import com.github.houbb.log.integration.core.Log;
import com.github.houbb.log.integration.core.LogFactory;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author WtMonster
 * @date 2023/4/30 20:24
 */
public class PrometheusConfig {
    private static final Log log = LogFactory.getLog(PrometheusConfig.class);

    public static final PrometheusMeterRegistry MICROMETER_REGISTRY;
    public static final CollectorRegistry REGISTRY;

    static {
        // 设置JVM监控
        MICROMETER_REGISTRY = new PrometheusMeterRegistry(io.micrometer.prometheus.PrometheusConfig.DEFAULT);
        MICROMETER_REGISTRY.config().commonTags("application", "mq-broker", "instance", "localhost:8001");
        new ClassLoaderMetrics().bindTo(MICROMETER_REGISTRY);
        new JvmMemoryMetrics().bindTo(MICROMETER_REGISTRY);
        new JvmGcMetrics().bindTo(MICROMETER_REGISTRY);
        new ProcessorMetrics().bindTo(MICROMETER_REGISTRY);
        new JvmThreadMetrics().bindTo(MICROMETER_REGISTRY);
        new UptimeMetrics().bindTo(MICROMETER_REGISTRY);
        new FileDescriptorMetrics().bindTo(MICROMETER_REGISTRY);

        Metrics.addRegistry(MICROMETER_REGISTRY);

        REGISTRY = MICROMETER_REGISTRY.getPrometheusRegistry();
    }


    public static void init() {
        try {
            new HTTPServer(new InetSocketAddress(8001), REGISTRY);
        } catch (IOException e) {
            log.info("prometheus监控启动失败，cause{}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

}
