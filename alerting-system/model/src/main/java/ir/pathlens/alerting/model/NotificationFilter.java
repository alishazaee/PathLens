package ir.pathlens.alerting.model;

import java.util.UUID;

/**
 * Filter criteria for searching notifications.
 */
public record NotificationFilter(
        Boolean seen,
        Boolean isActive,
        String title,
        UUID ruleId
) {

}
