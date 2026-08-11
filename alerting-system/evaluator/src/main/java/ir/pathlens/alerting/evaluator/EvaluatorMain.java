package ir.pathlens.alerting.evaluator;

import static ir.pathlens.alerting.evaluator.ConfigReader.loadConfig;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import ir.pathlens.alerting.evaluator.configs.ApplicationConfig;
import ir.pathlens.alerting.evaluator.configs.PostgresConfig;
import ir.pathlens.client.ApiCallException;
import java.io.IOException;
import java.nio.file.Path;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application entry point that wires up the evaluator and starts processing.
 */
public class EvaluatorMain {
    private static final Logger logger = LoggerFactory.getLogger(EvaluatorMain.class);

    public static void main(String[] args) throws ApiCallException {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "One required arguments (config path) should be provided.");
        }

        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            logger.error("Crashed because of unhandled exception", e);
            // Unlike the exit method, halt method does not cause shutdown hooks to be started.
            Runtime.getRuntime().halt(-1);
        });

        ApplicationConfig config = loadConfig(Path.of(args[0]));

        migrate(config.postgresConfig());

        PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        CompositeMeterRegistry meterRegistry = new CompositeMeterRegistry();
        meterRegistry.add(prometheusRegistry);


        HTTPServer httpServer = createHttpServer(config.prometheusPortNumber(), prometheusRegistry);
        Evaluator evaluator = new Evaluator(config, meterRegistry);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                evaluator.close();
            } catch (Exception e) {
                logger.error("graceful shutdown failed");
            }
            httpServer.stop();
        }));

        evaluator.start();
    }

    public static void migrate(PostgresConfig config) {

        Flyway flyway = Flyway.configure()
                .dataSource(
                        config.getUrl(),
                        config.getUsername(),
                        config.getPassword()
                )
                .locations("classpath:db/migrations")
                .load();

        flyway.migrate();
    }

    private static HTTPServer createHttpServer(
            int prometheusPort, PrometheusMeterRegistry registry) {
        try {
            return HTTPServer.builder()
                    .port(prometheusPort)
                    .registry(registry.getPrometheusRegistry())
                    .buildAndStart();
        } catch (IOException e) {
            throw new AssertionError("Failed to start Prometheus HTTP server on port " + prometheusPort, e);
        }
    }
}
