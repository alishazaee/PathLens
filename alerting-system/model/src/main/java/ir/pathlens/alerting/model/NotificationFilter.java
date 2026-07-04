package ir.pathlens.alerting.model;

import java.util.UUID;

public record NotificationFilter(
        Boolean seen,
        Boolean isActive,
        String title,
        UUID ruleId
) {

}
