package ir.pathlens.alerting.rest.service;

import ir.pathlens.alerting.model.Notification;
import ir.pathlens.alerting.model.NotificationFilter;
import ir.pathlens.alerting.rest.entity.NotificationEntity;
import ir.pathlens.alerting.rest.entity.RuleEntity;
import ir.pathlens.alerting.rest.filters.NotificationSpecification;
import ir.pathlens.alerting.rest.mappers.NotificationMapper;
import ir.pathlens.alerting.rest.repository.NotificationRepository;
import jakarta.transaction.Transactional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Service for notification operations.
 */
@Service
@Transactional
public class NotificationService {

    private final NotificationRepository notificationRepository;

    private static final Logger logger = LoggerFactory.getLogger(NotificationService.class);

    public NotificationService(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    public Notification getNotification(UUID id) {
        NotificationEntity notificationEntity = notificationRepository.findById(id).orElseThrow(
                () -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Notification with id " + id + " not found"));
        return NotificationMapper.toDto(notificationEntity);
    }

    public Notification setSeen(UUID id) {
        NotificationEntity notificationEntity = notificationRepository.findById(id).orElseThrow(
                () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Notification with id " + id + " not found"));
        notificationEntity.setSeen(true);
        return NotificationMapper.toDto(notificationEntity);
    }

    public Page<Notification> search(NotificationFilter filter, Pageable pageable) {
        Page<NotificationEntity> page = notificationRepository.findAll(NotificationSpecification.fromFilter(filter),
                pageable);
        return page.map(NotificationMapper::toDto);
    }

    void createNewNotification(RuleEntity rule) {
        NotificationEntity notificationEntity = new NotificationEntity();
        notificationEntity.setRule(rule);
        notificationRepository.save(notificationEntity);
        logger.info("successfully created notification for rule id %s".formatted(rule.getId()));
    }
}
