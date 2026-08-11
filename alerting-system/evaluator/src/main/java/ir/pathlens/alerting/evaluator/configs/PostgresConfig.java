package ir.pathlens.alerting.evaluator.configs;

/** Configuration for connecting to the PostgreSQL database. */
public class PostgresConfig {

    private String url;
    private String username;
    private String password;
    private int maximumPoolSize;
    private int minimumIdle;
    private long connectionTimeoutInMillis;
    private long idleTimeoutInMillis;
    private long maxLifetimeInMillis;

    public PostgresConfig() {
    }

    public PostgresConfig(String url, String username, String password, int maximumPoolSize, int minimumIdle,
            long connectionTimeoutInMillis, long idleTimeoutInMillis, long maxLifetimeInMillis) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.maximumPoolSize = maximumPoolSize;
        this.minimumIdle = minimumIdle;
        this.connectionTimeoutInMillis = connectionTimeoutInMillis;
        this.idleTimeoutInMillis = idleTimeoutInMillis;
        this.maxLifetimeInMillis = maxLifetimeInMillis;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }

    public int getMinimumIdle() {
        return minimumIdle;
    }

    public void setMinimumIdle(int minimumIdle) {
        this.minimumIdle = minimumIdle;
    }

    public long getConnectionTimeoutInMillis() {
        return connectionTimeoutInMillis;
    }

    public void setConnectionTimeoutInMillis(long connectionTimeoutInMillis) {
        this.connectionTimeoutInMillis = connectionTimeoutInMillis;
    }

    public long getIdleTimeoutInMillis() {
        return idleTimeoutInMillis;
    }

    public void setIdleTimeoutInMillis(long idleTimeoutInMillis) {
        this.idleTimeoutInMillis = idleTimeoutInMillis;
    }

    public long getMaxLifetimeInMillis() {
        return maxLifetimeInMillis;
    }

    public void setMaxLifetimeInMillis(long maxLifetimeInMillis) {
        this.maxLifetimeInMillis = maxLifetimeInMillis;
    }
}