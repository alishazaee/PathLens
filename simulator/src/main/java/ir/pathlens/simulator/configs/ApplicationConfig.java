package ir.pathlens.simulator.configs;

/**
 * YAML-deserialized configuration for the simulator.
 */
public class ApplicationConfig {

    private String bootstrapServers;
    private String topic;
    private int minBatchSize;
    private int maxBatchSize;
    private long intervalMillis;
    private long lingerMs;
    private int bufferMemory;
    private int batchSizeKafka;
    private String deviceApiUrl;
    private int numberOfDevices;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getMinBatchSize() {
        return minBatchSize;
    }

    public void setMinBatchSize(int minBatchSize) {
        this.minBatchSize = minBatchSize;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public long getIntervalMillis() {
        return intervalMillis;
    }

    public void setIntervalMillis(long intervalMillis) {
        this.intervalMillis = intervalMillis;
    }

    public long getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(long lingerMs) {
        this.lingerMs = lingerMs;
    }

    public int getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(int bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public int getBatchSizeKafka() {
        return batchSizeKafka;
    }

    public void setBatchSizeKafka(int batchSizeKafka) {
        this.batchSizeKafka = batchSizeKafka;
    }

    public String getDeviceApiUrl() {
        return deviceApiUrl;
    }

    public void setDeviceApiUrl(String deviceApiUrl) {
        this.deviceApiUrl = deviceApiUrl;
    }

    public int getNumberOfDevices() {
        return numberOfDevices;
    }

    public void setNumberOfDevices(int numberOfDevices) {
        this.numberOfDevices = numberOfDevices;
    }

    public void validate() {
        if (minBatchSize >= maxBatchSize) {
            throw new IllegalArgumentException("minBatchSize must be less than maxBatchSize");
        }
        if (minBatchSize <= 0) {
            throw new IllegalArgumentException("minBatchSize must be greater than 0");
        }
    }
}
