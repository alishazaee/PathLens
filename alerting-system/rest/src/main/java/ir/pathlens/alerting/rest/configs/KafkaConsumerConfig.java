package ir.pathlens.alerting.rest.configs;

import java.util.Map;

/**
 * Kafka consumer configuration properties.
 */
public record KafkaConsumerConfig(
        String bootstrapServers,
        String groupId,
        String autoOffsetReset,
        int maxConcurrency,
        int batchSize,
        long commitIntervalMs,
        Map<String, String> extraConfigs
) {
}