package ir.pathlens.alerting.rest.service;

import static ir.pathlens.alerting.db.jooq.tables.Rule.RULE;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.noCondition;

import ir.pathlens.alerting.db.jooq.tables.records.RuleRecord;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleCreateDto;
import ir.pathlens.alerting.model.RuleFilter;
import ir.pathlens.alerting.rest.mappers.RuleMapper;
import ir.pathlens.common.model.Page;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/**
 * Service for rule operations backed by jOOQ.
 */
@Service
@Transactional
public class RuleService {

    private static final ZoneId TEHRAN_ZONE = ZoneId.of("Asia/Tehran");

    private final DSLContext dsl;

    public RuleService(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Rule createRule(RuleCreateDto rule) {
        RuleRecord record = dsl.insertInto(RULE)
                .columns(RULE.TITLE, RULE.GEOMETRY_WKT, RULE.EXPIRES_AT,
                        RULE.IDENTITY_TYPE, RULE.IDENTITY_VALUE, RULE.RULE_TYPE)
                .values(rule.title(), rule.geometryWkt(), rule.expiresAt().truncatedTo(ChronoUnit.MICROS),
                        rule.identity().identityType().name(), rule.identity().identityValue(),
                        rule.ruleType().name())
                .returning()
                .fetchOne();
        return RuleMapper.toDto(record);
    }

    public Rule activateRule(UUID id) {
        return setActive(id, true);
    }

    public Rule deactivateRule(UUID id) {
        return setActive(id, false);
    }

    public Page<Rule> search(RuleFilter filter, int page, int size) {
        int safePage = Math.max(page, 0);
        int safeSize = Math.max(size, 1);

        Condition condition = noCondition();
        if (filter != null) {
            if (filter.isActive() != null) {
                condition = condition.and(RULE.IS_ACTIVE.eq(filter.isActive()));
            }
            if (filter.title() != null) {
                condition = condition.and(RULE.TITLE.eq(filter.title()));
            }
        }

        int total = dsl.fetchCount(RULE, condition);
        List<RuleRecord> records = dsl.selectFrom(RULE)
                .where(condition)
                .orderBy(RULE.CREATED_AT)
                .limit(safeSize)
                .offset((long) safePage * safeSize)
                .fetch();

        return Page.of(records.stream().map(RuleMapper::toDto).toList(), safePage, safeSize, total);
    }

    public List<Rule> getAllActiveRules() {
        return dsl.selectFrom(RULE)
                .where(RULE.IS_ACTIVE.eq(true))
                .and(RULE.EXPIRES_AT.gt(LocalDateTime.now()))
                .fetch()
                .stream()
                .map(RuleMapper::toDto)
                .toList();
    }

    public long getRevisionNumber() {
        Record1<LocalDateTime> maxUpdatedAt = dsl.select(max(RULE.UPDATED_AT)).from(RULE).fetchOne();
        if (maxUpdatedAt == null || maxUpdatedAt.value1() == null) {
            return 0;
        }
        LocalDateTime updatedAt = maxUpdatedAt.value1();
        ZoneOffset offset = TEHRAN_ZONE.getRules().getOffset(updatedAt);
        return updatedAt.toEpochSecond(offset);
    }

    private Rule setActive(UUID id, boolean active) {
        int updated = dsl.update(RULE)
                .set(RULE.IS_ACTIVE, active)
                .set(RULE.UPDATED_AT, LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS))
                .where(RULE.ID.eq(id))
                .execute();
        if (updated == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Rule with id " + id + " not found");
        }
        return findById(id);
    }

    private Rule findById(UUID id) {
        RuleRecord record = dsl.selectFrom(RULE).where(RULE.ID.eq(id)).fetchOne();
        if (record == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Rule with id " + id + " not found");
        }
        return RuleMapper.toDto(record);
    }
}
