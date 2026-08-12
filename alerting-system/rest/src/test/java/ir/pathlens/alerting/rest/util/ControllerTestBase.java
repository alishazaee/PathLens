package ir.pathlens.alerting.rest.util;

import ir.pathlens.extension.postgresql.SpringCommonPostgresConfigs;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Base class for the alerting REST integration tests.
 *
 * <p>Wires the shared PostgreSQL test container into the Spring environment.
 */
public abstract class ControllerTestBase implements SpringCommonPostgresConfigs {

    @DynamicPropertySource
    static void register(DynamicPropertyRegistry registry) {
        SpringCommonPostgresConfigs.registerPostgresProperties(registry);
    }
}
