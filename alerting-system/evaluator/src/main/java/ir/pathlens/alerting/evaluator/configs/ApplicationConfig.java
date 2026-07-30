package ir.pathlens.alerting.evaluator.configs;

/** Configuration for the evaluator application. */
public record ApplicationConfig(KafkaConsumerConfig kafkaConsumerConfig, String sourceTopic, int threadCount,
                                String destinationTopic, KafkaProducerConfig kafkaProducerConfig,
                                int maxConcurrency, int prometheusPortNumber, RulesCacheConfig rulesCacheConfig) {
}
