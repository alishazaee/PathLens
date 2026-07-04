package ir.pathlens.alerting.rest.repository;

import ir.pathlens.alerting.rest.entity.LogEntity;
import ir.pathlens.alerting.rest.entity.LogEntityId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

/**
 * Repository for target log entities.
 */
public interface LogRepository extends
        JpaRepository<LogEntity, LogEntityId>,
        JpaSpecificationExecutor<LogEntity> {
}