package ir.pathlens.alerting.rest.util;

import ir.pathlens.extension.postgresql.SpringCommonPostgresConfigs;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Shared configuration for alerting-system integration tests. */
public class CommonConfigs implements SpringCommonPostgresConfigs, CommonKafkaConfigs {

    @DynamicPropertySource
    static void register(DynamicPropertyRegistry registry) {
        CommonKafkaConfigs.registerKafkaProperties(registry);
        SpringCommonPostgresConfigs.registerPostgresProperties(registry);
    }
}
