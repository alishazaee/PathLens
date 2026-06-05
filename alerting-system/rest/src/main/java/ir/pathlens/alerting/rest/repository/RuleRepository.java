package ir.pathlens.alerting.rest.repository;

import ir.pathlens.alerting.rest.entity.RuleEntity;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

/**
 * Repository for rule entities.
 */
public interface RuleRepository extends JpaRepository<RuleEntity, UUID>, JpaSpecificationExecutor<RuleEntity> {

    List<RuleEntity> findByIsActiveTrueAndExpiresAtAfter(LocalDateTime time);

    List<RuleEntity> findByIdInAndIsActiveTrueAndExpiresAtAfter(List<UUID> id, LocalDateTime time);

    Optional<RuleEntity> findTopByOrderByUpdatedAtDesc();
}
