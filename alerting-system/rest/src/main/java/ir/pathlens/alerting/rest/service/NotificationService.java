package ir.pathlens.alerting.rest.service;

import static ir.pathlens.alerting.db.jooq.tables.Notification.NOTIFICATION;
import static ir.pathlens.alerting.db.jooq.tables.Rule.RULE;
import static org.jooq.impl.DSL.noCondition;

import ir.pathlens.alerting.db.jooq.tables.records.NotificationRecord;
import ir.pathlens.alerting.db.jooq.tables.records.RuleRecord;
import ir.pathlens.alerting.model.Notification;
import ir.pathlens.alerting.model.NotificationFilter;
import ir.pathlens.alerting.rest.mappers.NotificationMapper;
import ir.pathlens.common.model.Page;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/**
 * Service for notification operations backed by jOOQ.
 */
@Service
@Transactional
public class NotificationService {

    private final DSLContext dsl;

    public NotificationService(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Notification getNotification(UUID id) {
        NotificationRecord notification = dsl.selectFrom(NOTIFICATION)
                .where(NOTIFICATION.ID.eq(id))
                .fetchOne();
        if (notification == null) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "Notification with id " + id + " not found");
        }
        return NotificationMapper.toDto(notification, findRule(notification.getRuleId()));
    }

    public Notification setSeen(UUID id) {
        int updated = dsl.update(NOTIFICATION)
                .set(NOTIFICATION.SEEN, true)
                .where(NOTIFICATION.ID.eq(id))
                .execute();
        if (updated == 0) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "Notification with id " + id + " not found");
        }
        return getNotification(id);
    }

    public Page<Notification> search(NotificationFilter filter, int page, int size) {
        int safePage = Math.max(page, 0);
        int safeSize = Math.max(size, 1);

        Condition condition = noCondition();
        if (filter != null) {
            if (filter.seen() != null) {
                condition = condition.and(NOTIFICATION.SEEN.eq(filter.seen()));
            }
            if (filter.ruleId() != null) {
                condition = condition.and(NOTIFICATION.RULE_ID.eq(filter.ruleId()));
            }
            if (filter.isActive() != null) {
                condition = condition.and(RULE.IS_ACTIVE.eq(filter.isActive()))
                        .and(RULE.EXPIRES_AT.gt(LocalDateTime.now()));
            }
            if (filter.title() != null) {
                condition = condition.and(RULE.TITLE.eq(filter.title()));
            }
        }

        int total = dsl.fetchCount(NOTIFICATION.join(RULE).on(NOTIFICATION.RULE_ID.eq(RULE.ID)), condition);
        // The columns must stay unaliased so that into(Table) extracts them by field identity,
        // which is the only unambiguous way to separate the duplicate id/created_at columns.
        List<Notification> content = dsl.select(NOTIFICATION.fields())
                .select(RULE.fields())
                .from(NOTIFICATION.join(RULE).on(NOTIFICATION.RULE_ID.eq(RULE.ID)))
                .where(condition)
                .orderBy(NOTIFICATION.CREATED_AT)
                .limit(safeSize)
                .offset((long) safePage * safeSize)
                .fetch()
                .stream()
                .map(row -> NotificationMapper.toDto(row.into(NOTIFICATION), row.into(RULE)))
                .toList();

        return Page.of(content, safePage, safeSize, total);
    }

    private RuleRecord findRule(UUID ruleId) {
        return dsl.selectFrom(RULE).where(RULE.ID.eq(ruleId)).fetchOne();
    }
}
