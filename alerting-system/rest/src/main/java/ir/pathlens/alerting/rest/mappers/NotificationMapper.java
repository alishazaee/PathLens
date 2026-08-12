package ir.pathlens.alerting.rest.mappers;

import ir.pathlens.alerting.db.jooq.tables.records.NotificationRecord;
import ir.pathlens.alerting.db.jooq.tables.records.RuleRecord;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.Notification;
import ir.pathlens.alerting.model.NotificationMessage;
import ir.pathlens.alerting.model.RuleType;

/**
 * Mapper for notification models.
 */
public class NotificationMapper {

    public static Notification toDto(NotificationRecord notification, RuleRecord rule) {
        IdentityWrapper identity = new IdentityWrapper(
                IdentityType.valueOf(rule.getIdentityType()),
                rule.getIdentityValue()
        );

        String identityValue = switch (identity.identityType()) {
            case PhoneNumber -> "PHONE NUMBER " + identity.identityValue();
            case PlateNumber -> "PLATE NUMBER " + identity.identityValue();
        };

        String message = switch (RuleType.valueOf(rule.getRuleType())) {
            case Enter -> String.format(NotificationMessage.USER_ENTERED_THE_ZONE, identityValue);
            case Exit -> String.format(NotificationMessage.USER_EXISTED_THE_ZONE, identityValue);
        };

        return new Notification(
                notification.getId(),
                notification.getCreatedAt(),
                message,
                rule.getId(),
                notification.getSeen(),
                rule.getIsActive()
        );
    }
}
