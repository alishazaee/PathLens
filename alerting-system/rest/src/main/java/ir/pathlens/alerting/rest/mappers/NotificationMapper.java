package ir.pathlens.alerting.rest.mappers;

import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.Notification;
import ir.pathlens.alerting.model.NotificationMessage;
import ir.pathlens.alerting.model.RuleType;
import ir.pathlens.alerting.rest.entity.NotificationEntity;

/**
 * Mapper for notification models.
 */
public class NotificationMapper {
    public static Notification toDto(NotificationEntity notificationEntity) {

        IdentityWrapper identity = notificationEntity.getRule().getIdentity();

        IdentityType identityType = identity.identityType();
        RuleType ruleType = notificationEntity.getRule().getRuleType();

        String identityTypeStr = switch (identityType) {
            case PhoneNumber -> "PHONE NUMBER ";
            case PlateNumber -> "PLATE NUMBER ";
        };

        String identityValue = identityTypeStr + identity.identityValue();

        String message = switch (ruleType) {
            case Enter -> String.format(NotificationMessage.USER_ENTERED_THE_ZONE, identityValue);
            case Exit -> String.format(NotificationMessage.USER_EXISTED_THE_ZONE, identityValue);
        };

        return new Notification(notificationEntity.getId(),
                notificationEntity.getCreatedAt(),
                message,
                notificationEntity.getRule().getId(),
                notificationEntity.isSeen(),
                notificationEntity.getRule().isActive());
    }
}
