package broker;

import com.sun.net.httpserver.HttpServer;
import com.water.mq.broker.core.MqBroker;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.Counter;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * @author WtMonster
 * @date 2022/11/29 1:14
 */
public class BrokerMain {
    public static final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    static {
        // 添加 Prometheus 全局 Label，建议加一上对应的应用名
        registry.config().commonTags("application", " mq-broker");
    }

    static final Counter requests = Counter.build()
            .name("requests_total").help("Total requests.").register();

    static void processRequest() {
        requests.inc();
        // Your code here.
    }
    public static void main(String[] args) throws IOException {

        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);
        new FileDescriptorMetrics().bindTo(registry);

        try {
            // 暴露 Prometheus HTTP 服务，如果已经有，可以使用已有的 HTTP Server
            HttpServer server = HttpServer.create(new InetSocketAddress(8001), 0);
            server.createContext("/metrics", httpExchange -> {
                String response = registry.scrape();

                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });


            new Thread(server::start).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        processRequest();
        MqBroker broker = new MqBroker();
        broker.start();
    }
}
