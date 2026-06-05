package ir.pathlens.alerting.rest.filters;

import ir.pathlens.alerting.model.LogFilter;
import ir.pathlens.alerting.rest.entity.LogEntity;
import jakarta.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.jpa.domain.Specification;

/**
 * Specification builder for target log filters.
 */
public class LogSpecification {

    public static Specification<LogEntity> withFilter(LogFilter filter) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (filter != null) {

                if (filter.isViolated() != null) {
                    predicates.add(
                            cb.equal(root.get("isViolated"), filter.isViolated())
                    );
                }

                if (filter.ruleId() != null) {
                    predicates.add(
                            cb.equal(root.get("rule").get("id"), filter.ruleId())
                    );
                }
            }

            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }

}