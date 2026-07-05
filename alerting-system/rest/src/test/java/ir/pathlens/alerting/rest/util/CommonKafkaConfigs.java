package ir.pathlens.alerting.rest.util;

import static ir.pathlens.extension.kafka.KafkaExtension.getKafkaContainer;

import org.springframework.test.context.DynamicPropertyRegistry;

/** Kafka configuration for integration tests. */
public interface CommonKafkaConfigs {

    static DynamicPropertyRegistry registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("app.target-log-consumer.bootstrap-servers", getKafkaContainer()::getBootstrapServers);
        return registry;
    }
}
