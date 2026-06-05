package ir.pathlens.alerting.rest.configs;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

/**
 * Tells spring to enable auditing.
 */
@Configuration
@EnableJpaAuditing
public class JpaAuditingConfiguration {
}