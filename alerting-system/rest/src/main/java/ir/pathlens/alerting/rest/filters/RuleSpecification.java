package ir.pathlens.alerting.rest.filters;

import ir.pathlens.alerting.model.RuleFilter;
import ir.pathlens.alerting.rest.entity.RuleEntity;
import jakarta.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.jpa.domain.Specification;

/**
 * Specification class for {@link RuleEntity} entity to filter based on different fields.
 */
public class RuleSpecification {

    public static Specification<RuleEntity> fromFilter(RuleFilter filter) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (filter == null) {
                return cb.and(predicates.toArray(new Predicate[0]));
            }

            if (filter.isActive() != null) {
                predicates.add(cb.equal(root.get(("isActive")), filter.isActive()));
            }
            if (filter.title() != null) {
                predicates.add(cb.equal(root.get(("title")), filter.title()));
            }
            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }
}
