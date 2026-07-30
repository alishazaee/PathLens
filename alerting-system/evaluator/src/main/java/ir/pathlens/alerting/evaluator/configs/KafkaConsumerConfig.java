package ir.pathlens.alerting.evaluator.configs;

import java.util.Map;

/** Kafka consumer configuration. */
public class KafkaConsumerConfig {

    private String bootstrapServers;
    private String autoOffsetReset;
    private int queueSize;
    private String groupId;
    private Map<String, String> extraConfigs;

    public KafkaConsumerConfig() {
    }

    public KafkaConsumerConfig(String bootstrapServers, String autoOffsetReset, int queueSize,
                               String groupId, Map<String, String> extraConfigs) {
        this.bootstrapServers = bootstrapServers;
        this.autoOffsetReset = autoOffsetReset;
        this.queueSize = queueSize;
        this.groupId = groupId;
        this.extraConfigs = extraConfigs;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Map<String, String> getExtraConfigs() {
        return extraConfigs;
    }

    public void setExtraConfigs(Map<String, String> extraConfigs) {
        this.extraConfigs = extraConfigs;
    }
}
