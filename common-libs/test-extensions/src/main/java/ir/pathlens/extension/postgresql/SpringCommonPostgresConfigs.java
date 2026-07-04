package ir.pathlens.extension.postgresql;

import static ir.pathlens.extension.postgresql.PostgresqlExtension.getPostgresqlContainer;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Provides dynamic Spring property configuration for PostgreSQL test containers.
 */
public class SpringCommonPostgresConfigs {

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", getPostgresqlContainer()::getJdbcUrl);
        registry.add("spring.datasource.username", getPostgresqlContainer()::getUsername);
        registry.add("spring.datasource.password", getPostgresqlContainer()::getPassword);

        registry.add("spring.flyway.enabled", () -> true);
        registry.add("spring.flyway.url", getPostgresqlContainer()::getJdbcUrl);
        registry.add("spring.flyway.user", getPostgresqlContainer()::getUsername);
        registry.add("spring.flyway.password", getPostgresqlContainer()::getPassword);
    }
}
