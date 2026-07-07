package ir.pathlens.alerting.evaluator.configs;

/** Rules cache configuration for fetching and refreshing alerting rules. */
public class RulesCacheConfig {

    private String baseUrl;
    private int minInitialDelayInMillis;
    private int maxInitialDelayInMillis;

    public RulesCacheConfig() {
    }

    public RulesCacheConfig(String baseUrl, int minInitialDelayInMillis, int maxInitialDelayInMillis) {
        this.baseUrl = baseUrl;
        this.minInitialDelayInMillis = minInitialDelayInMillis;
        this.maxInitialDelayInMillis = maxInitialDelayInMillis;
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
}
