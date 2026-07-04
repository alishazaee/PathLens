package ir.pathlens.extension.postgresql;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/**
 * JUnit5 extension that starts a PostgreSQL container before all tests.
 */
public class PostgresqlExtension implements BeforeAllCallback {

    @Container
    static final PostgreSQLContainer<?> postgreSQLContainer =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"))
                    .withDatabaseName("test")
                    .withUsername("test")
                    .withPassword("test");

    public static PostgreSQLContainer<?> getPostgresqlContainer() {
        return postgreSQLContainer;
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        if (!postgreSQLContainer.isRunning()) {
            postgreSQLContainer.start();

            Runtime.getRuntime().addShutdownHook(new Thread(postgreSQLContainer::stop));
        }
    }
}
