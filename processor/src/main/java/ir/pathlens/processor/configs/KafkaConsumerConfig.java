package ir.pathlens.processor.configs;

import java.util.Map;

/**
 * Configuration for the Kafka consumer.
 */
public class KafkaConsumerConfig {
    private String bootstrapServers;
    private String autoOffsetReset;
    private String groupId;
    private Map<String, String> extraConfigs;

    public KafkaConsumerConfig(String bootstrapServers, String autoOffsetReset, String groupId,
            Map<String, String> extraConfigs) {
        this.bootstrapServers = bootstrapServers;
        this.autoOffsetReset = autoOffsetReset;
        this.groupId = groupId;
        this.extraConfigs = extraConfigs;
    }

    public KafkaConsumerConfig() {
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

    @Override
    public String toString() {
        return "KafkaConsumerConfig{"
                + "bootstrapServers='" + bootstrapServers + '\''
                + ", autoOffsetReset='" + autoOffsetReset + '\''
                + ", groupId='" + groupId + '\''
                + ", extraConfigs=" + (extraConfigs == null ? "null" : extraConfigs.keySet())
                + '}';
    }

}
