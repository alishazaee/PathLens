package ir.pathlens.alerting.evaluator.configs;

/** Configuration for the evaluator application. */
public record ApplicationConfig(
        KafkaConsumerConfig kafkaConsumerConfig, String sourceTopic, PostgresConfig postgresConfig,
        int threadCount, String destinationTopic, KafkaProducerConfig kafkaProducerConfig,
        int maxConcurrency, int prometheusPortNumber, RulesCacheConfig rulesCacheConfig,
        int persisterBatchSize, int persistQueueSize, long persistBatchTimeOutInMillis) {
}
