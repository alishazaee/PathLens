package ir.pathlens.alerting.model;

import java.util.UUID;

/**
 * Filter criteria for searching target logs.
 */
public record LogFilter(
        Boolean isViolated,
        UUID ruleId
) {
}
