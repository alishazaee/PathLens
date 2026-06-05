package ir.pathlens.alerting.model;

/**
 * Notification message templates.
 */
public class NotificationMessage {
    private NotificationMessage() {
        // This is a utility class and can't be initiated
    }

    public static final String USER_ENTERED_THE_ZONE = "%s has entered the zone";
    public static final String USER_EXISTED_THE_ZONE = "%s has exited the zone";
}
