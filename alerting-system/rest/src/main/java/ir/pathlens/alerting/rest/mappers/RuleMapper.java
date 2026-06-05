package ir.pathlens.alerting.rest.mappers;

import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleCreateDto;
import ir.pathlens.alerting.rest.entity.RuleEntity;

/**
 * Mapper for rule models.
 */
public class RuleMapper {
    public static RuleEntity fromDto(RuleCreateDto rule) {
        RuleEntity ruleEntity = new RuleEntity();

        ruleEntity.setTitle(rule.title());
        ruleEntity.setGeometryWkt(rule.geometryWkt());
        ruleEntity.setIdentity(
                new IdentityWrapper(
                        rule.identity().identityType(),
                        rule.identity().identityValue()
                )
        );
        ruleEntity.setExpiresAt(rule.expiresAt());
        ruleEntity.setRuleType(rule.ruleType());

        return ruleEntity;
    }

    public static Rule toDto(RuleEntity rule) {
        return new Rule(
                rule.getId(),
                rule.getTitle(),
                rule.getGeometryWkt(),
                rule.getExpiresAt(),
                new IdentityWrapper(rule.getIdentity().identityType(), rule.getIdentity().identityValue()),
                rule.isActive(),
                rule.getRuleType(),
                rule.isViolated()
        );
    }
}
