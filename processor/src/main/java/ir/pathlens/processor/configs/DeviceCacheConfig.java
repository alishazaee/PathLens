package ir.pathlens.processor.configs;

/**
 * Configuration for the device cache client.
 */
public class DeviceCacheConfig {
    private String baseUrl;
    private int minInitialDelayInMillis;
    private int maxInitialDelayInMillis;
    private int syncIntervalInMillis;

    public DeviceCacheConfig(String baseUrl, int minInitialDelayInMillis, int maxInitialDelayInMillis,
            int syncIntervalInMillis) {
        this.baseUrl = baseUrl;
        this.minInitialDelayInMillis = minInitialDelayInMillis;
        this.maxInitialDelayInMillis = maxInitialDelayInMillis;
        this.syncIntervalInMillis = syncIntervalInMillis;
    }

    public DeviceCacheConfig() {
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public int getMinInitialDelayInMillis() {
        return minInitialDelayInMillis;
    }

    public void setMinInitialDelayInMillis(int minInitialDelayInMillis) {
        this.minInitialDelayInMillis = minInitialDelayInMillis;
    }

    public int getMaxInitialDelayInMillis() {
        return maxInitialDelayInMillis;
    }

    public void setMaxInitialDelayInMillis(int maxInitialDelayInMillis) {
        this.maxInitialDelayInMillis = maxInitialDelayInMillis;
    }

    public int getSyncIntervalInMillis() {
        return syncIntervalInMillis;
    }

    public void setSyncIntervalInMillis(int syncIntervalInMillis) {
        this.syncIntervalInMillis = syncIntervalInMillis;
    }
}
