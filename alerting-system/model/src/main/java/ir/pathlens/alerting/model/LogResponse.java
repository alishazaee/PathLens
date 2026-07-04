package ir.pathlens.alerting.model;

import java.util.UUID;

/**
 * Response payload for target log search results.
 */
public record LogResponse(
        UUID id,
        boolean isViolated,
        UUID ruleId,
        double latitude,
        double longitude
) {}