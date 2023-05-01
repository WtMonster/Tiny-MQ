package com.water.mq.producer.prometheus;

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

    public static final CollectorRegistry REGISTRY;

    static {
        // 设置JVM监控
        PrometheusMeterRegistry micrometerRegistry =
                new PrometheusMeterRegistry(io.micrometer.prometheus.PrometheusConfig.DEFAULT);
        micrometerRegistry.config().commonTags("application", "mq-producer", "instance", "localhost:8003");
        new ClassLoaderMetrics().bindTo(micrometerRegistry);
        new JvmMemoryMetrics().bindTo(micrometerRegistry);
        new JvmGcMetrics().bindTo(micrometerRegistry);
        new ProcessorMetrics().bindTo(micrometerRegistry);
        new JvmThreadMetrics().bindTo(micrometerRegistry);
        new UptimeMetrics().bindTo(micrometerRegistry);
        new FileDescriptorMetrics().bindTo(micrometerRegistry);

        Metrics.addRegistry(micrometerRegistry);

        REGISTRY = micrometerRegistry.getPrometheusRegistry();
    }


    public static void init() {
        try {
            new HTTPServer(new InetSocketAddress(8003), REGISTRY);
        } catch (IOException e) {
            log.info("prometheus监控启动失败，cause{}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

}
