package ir.pathlens.alerting.evaluator.configs;

import java.util.Map;

/** Kafka producer configuration. */
public class KafkaProducerConfig {

    private Map<String, String> extraConfigs;
    private String bootstrapServers;

    public KafkaProducerConfig() {
    }

    public KafkaProducerConfig(Map<String, String> extraConfigs, String bootstrapServers) {
        this.extraConfigs = extraConfigs;
        this.bootstrapServers = bootstrapServers;
    }

    public Map<String, String> getExtraConfigs() {
        return extraConfigs;
    }

    public void setExtraConfigs(Map<String, String> extraConfigs) {
        this.extraConfigs = extraConfigs;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
}
