package ir.pathlens.alerting.rest.filters;

import ir.pathlens.alerting.model.NotificationFilter;
import ir.pathlens.alerting.rest.entity.NotificationEntity;
import jakarta.persistence.criteria.Predicate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.jpa.domain.Specification;

/**
 * Specification builder for notification filters.
 */
public class NotificationSpecification {

    public static Specification<NotificationEntity> fromFilter(NotificationFilter filter) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (filter == null) {
                return cb.and(predicates.toArray(new Predicate[0]));
            }

            if (filter.isActive() != null) {
                predicates.add(
                        cb.and(
                                cb.equal(root.get("rule").get("isActive"), filter.isActive()),
                                cb.greaterThan(root.get("rule").get("expiresAt"), LocalDateTime.now())
                        )
                );
            }
            if (filter.seen() != null) {
                predicates.add(cb.equal(root.get(("seen")), filter.seen()));
            }
            if (filter.title() != null) {
                predicates.add(cb.equal(root.get("rule").get("title"), filter.title()));
            }
            if (filter.ruleId() != null) {
                predicates.add(cb.equal(root.get("rule").get("id"), filter.ruleId()));
            }
            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }
}
