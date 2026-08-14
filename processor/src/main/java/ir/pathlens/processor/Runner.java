package ir.pathlens.processor;

import static ir.pathlens.processor.ConfigReader.loadConfig;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import ir.pathlens.processor.configs.ApplicationConfig;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point that reads the application config, starts the metrics server and runs the processor.
 */
public class Runner {

    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    private Runner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "One required arguments (config path) should be provided.");
        }
        String configPath = args[0];

        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            logger.error("Crashed because of unhandled exception", e);
            Runtime.getRuntime().halt(-1);
        });

        logger.info("Starting camera log processor application...");
        ApplicationConfig config = loadConfig(Paths.get(configPath));

        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        startMetricsServer(meterRegistry, config.getPrometheusPortNumber());

        Processor processor = new Processor(meterRegistry, config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("JVM is Shutting down....");
            processor.close();
            logger.info("Bye!");
        }));

        processor.start();
        logger.info("Processor started successfully.");
    }

    private static void startMetricsServer(PrometheusMeterRegistry registry, int port) throws Exception {

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/metrics", exchange -> {
            byte[] response = registry.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, response.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });

        server.setExecutor(null);
        server.start();

        logger.info("Prometheus metrics exposed at http://localhost:{}/metrics", port);
    }
}
