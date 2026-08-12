package ir.pathlens.alerting.rest.mappers;

import ir.pathlens.alerting.db.jooq.tables.records.RuleRecord;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleType;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Mapper for rule models.
 */
public class RuleMapper {

    public static Rule toDto(RuleRecord rule) {
        return new Rule(
                rule.getId(),
                rule.getTitle(),
                rule.getGeometryWkt(),
                truncateToMicros(rule.getExpiresAt()),
                new IdentityWrapper(IdentityType.valueOf(rule.getIdentityType()), rule.getIdentityValue()),
                rule.getIsActive(),
                RuleType.valueOf(rule.getRuleType()),
                rule.getIsViolated(),
                truncateToMicros(rule.getCreatedAt())
        );
    }

    private static LocalDateTime truncateToMicros(LocalDateTime dateTime) {
        return dateTime.truncatedTo(ChronoUnit.MICROS);
    }
}
