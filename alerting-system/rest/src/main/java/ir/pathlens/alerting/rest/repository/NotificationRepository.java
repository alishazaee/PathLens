package ir.pathlens.alerting.rest.repository;

import ir.pathlens.alerting.rest.entity.NotificationEntity;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

/**
 * Repository for notification entities.
 */
public interface NotificationRepository
        extends JpaRepository<NotificationEntity, UUID>, JpaSpecificationExecutor<NotificationEntity> {

}
