package ir.pathlens.alerting.model;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Rule response model.
 */
public record Rule(
        UUID id,
        String title,
        String geometryWkt,
        LocalDateTime expiresAt,
        IdentityWrapper identity,
        boolean isActive,
        RuleType ruleType,
        boolean isViolated
) {}

