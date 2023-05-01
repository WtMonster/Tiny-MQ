package com.water.mq.broker;

import com.water.mq.broker.core.MqBroker;
import com.water.mq.broker.prometheus.PrometheusConfig;

import java.io.IOException;

/**
 * @author WtMonster
 * @date 2022/11/29 1:14
 */
public class BrokerMain {
    // public static final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    //
    // static {
    //     // 添加 Prometheus 全局 Label，建议加一上对应的应用名
    //     registry.config().commonTags("application", "mq-broker");
    //
    // }
    //
    // static final Counter requests = Counter.build()
    //         .name("requests_total").help("Total requests.").register(registry.getPrometheusRegistry());
    //
    // static void processRequest() {
    //     requests.inc();
    //     // Your code here.
    // }

    public static void main(String[] args) throws IOException {

        // new ClassLoaderMetrics().bindTo(registry);
        // new JvmMemoryMetrics().bindTo(registry);
        // new JvmGcMetrics().bindTo(registry);
        // new ProcessorMetrics().bindTo(registry);
        // new JvmThreadMetrics().bindTo(registry);
        // new UptimeMetrics().bindTo(registry);
        // new FileDescriptorMetrics().bindTo(registry);
        //
        //
        // new HTTPServer(new InetSocketAddress(8001), registry.getPrometheusRegistry());



        // Counter.builder("mq.broker.request")
        //         .description("The number of requests.")
        //         .register(registry);
        //
        // BrokerMain.registry.get("mq.broker.request").counter().increment();
        //
        // try {
        //     // 暴露 Prometheus HTTP 服务，如果已经有，可以使用已有的 HTTP Server
        //     HttpServer server = HttpServer.create(new InetSocketAddress(8001), 0);
        //
        //     server.createContext("/metrics", httpExchange -> {
        //         String response = registry.scrape();
        //
        //         httpExchange.sendResponseHeaders(200, response.getBytes().length);
        //         try (OutputStream os = httpExchange.getResponseBody()) {
        //             os.write(response.getBytes());
        //         }
        //     });
        //
        //
        //     new Thread(server::start).start();
        // } catch (IOException e) {
        //     throw new RuntimeException(e);
        // }

        // processRequest();

        PrometheusConfig.init();
        MqBroker broker = new MqBroker();
        broker.start();
    }
}
