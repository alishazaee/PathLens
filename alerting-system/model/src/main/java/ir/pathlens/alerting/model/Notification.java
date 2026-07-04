package ir.pathlens.alerting.model;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Notification response model.
 */
public record Notification(
        UUID id,
        LocalDateTime timestamp,
        String message,
        UUID ruleId,
        boolean seen,
        boolean isActive
) {

}
