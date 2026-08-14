package ir.pathlens.processor.configs;

import java.util.Map;

/**
 * Configuration for the Kafka producer.
 */
public class KafkaProducerConfig {
    private String bootstrapServers;
    private Map<String, String> extraConfigs;

    public KafkaProducerConfig(String bootstrapServers, Map<String, String> extraConfigs) {
        this.bootstrapServers = bootstrapServers;
        this.extraConfigs = extraConfigs;
    }

    public KafkaProducerConfig() {
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Map<String, String> getExtraConfigs() {
        return extraConfigs;
    }

    public void setExtraConfigs(Map<String, String> extraConfigs) {
        this.extraConfigs = extraConfigs;
    }
}