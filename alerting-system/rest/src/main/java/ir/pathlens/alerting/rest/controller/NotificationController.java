package ir.pathlens.alerting.rest.controller;

import ir.pathlens.alerting.model.ApiPathConstants;
import ir.pathlens.alerting.model.Notification;
import ir.pathlens.alerting.model.NotificationFilter;
import ir.pathlens.alerting.rest.service.NotificationService;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for notification service.
 */
@RestController
public class NotificationController {

    private final NotificationService notificationService;

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @GetMapping(ApiPathConstants.GET_NOTIFICATION_PATH)
    public Notification getNotification(@PathVariable String id) {
        return notificationService.getNotification(UUID.fromString(id));
    }

    @PatchMapping(ApiPathConstants.SET_NOTIFICATION_SEEN_PATH)
    public Notification setSeen(@PathVariable String id) {
        return notificationService.setSeen(UUID.fromString(id));
    }

    @GetMapping(ApiPathConstants.SEARCH_NOTIFICATIONS_PATH)
    public Page<Notification> search(NotificationFilter filter, Pageable pageable) {
        return notificationService.search(filter, pageable);
    }
}
