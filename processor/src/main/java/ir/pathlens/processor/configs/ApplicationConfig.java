package ir.pathlens.processor.configs;

/**
 * Top level configuration of the application.
 */
public class ApplicationConfig {

    private KafkaConsumerConfig kafkaConsumerConfig;
    private String sourceTopic;
    private String trashTopic;
    private String destinationTopic;
    private DeviceCacheConfig deviceCacheConfig;
    private KafkaProducerConfig kafkaProducerConfig;
    private int threadCount;
    private int queueSize;
    private int prometheusPortNumber;

    public ApplicationConfig(KafkaConsumerConfig kafkaConsumerConfig, String sourceTopic, String trashTopic,
            String destinationTopic, DeviceCacheConfig deviceCacheConfig, KafkaProducerConfig kafkaProducerConfig,
            int threadCount, int queueSize, int prometheusPortNumber) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.sourceTopic = sourceTopic;
        this.trashTopic = trashTopic;
        this.destinationTopic = destinationTopic;
        this.deviceCacheConfig = deviceCacheConfig;
        this.kafkaProducerConfig = kafkaProducerConfig;
        this.threadCount = threadCount;
        this.queueSize = queueSize;
        this.prometheusPortNumber = prometheusPortNumber;
    }

    public ApplicationConfig() {
    }

    public DeviceCacheConfig getDeviceCacheConfig() {
        return deviceCacheConfig;
    }

    public void setDeviceCacheConfig(DeviceCacheConfig deviceCacheConfig) {
        this.deviceCacheConfig = deviceCacheConfig;
    }

    public KafkaConsumerConfig getKafkaConsumerConfig() {
        return kafkaConsumerConfig;
    }

    public void setKafkaConsumerConfig(KafkaConsumerConfig kafkaConsumerConfig) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public String getTrashTopic() {
        return trashTopic;
    }

    public void setTrashTopic(String trashTopic) {
        this.trashTopic = trashTopic;
    }

    public String getDestinationTopic() {
        return destinationTopic;
    }

    public void setDestinationTopic(String destinationTopic) {
        this.destinationTopic = destinationTopic;
    }

    public KafkaProducerConfig getKafkaProducerConfig() {
        return kafkaProducerConfig;
    }

    public void setKafkaProducerConfig(KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getPrometheusPortNumber() {
        return prometheusPortNumber;
    }

    public void setPrometheusPortNumber(int prometheusPortNumber) {
        this.prometheusPortNumber = prometheusPortNumber;
    }
}
