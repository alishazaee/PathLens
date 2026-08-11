package ir.pathlens.alerting.rest.service;

import static org.jooq.impl.DSL.noCondition;

import ir.pathlens.alerting.db.jooq.tables.TrackedLog;
import ir.pathlens.alerting.db.jooq.tables.records.TrackedLogRecord;
import ir.pathlens.alerting.model.LogFilter;
import ir.pathlens.alerting.model.LogResponse;
import ir.pathlens.alerting.rest.mappers.LogMapper;
import ir.pathlens.common.model.Page;
import java.util.List;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service for target log operations backed by jOOQ.
 */
@Service
@Transactional(readOnly = true)
public class LogService {

    private final DSLContext dsl;

    public LogService(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Page<LogResponse> search(LogFilter filter, int page, int size) {
        int safePage = Math.max(page, 0);
        int safeSize = Math.max(size, 1);

        TrackedLog trackedLog = TrackedLog.TRACKED_LOG;
        Condition condition = noCondition();
        if (filter != null) {
            if (filter.isViolated() != null) {
                condition = condition.and(trackedLog.IS_VIOLATED.eq(filter.isViolated()));
            }
            if (filter.ruleId() != null) {
                condition = condition.and(trackedLog.RULE_ID.eq(filter.ruleId()));
            }
        }

        int total = dsl.fetchCount(trackedLog, condition);
        List<TrackedLogRecord> records = dsl.selectFrom(trackedLog)
                .where(condition)
                .orderBy(TrackedLog.TRACKED_LOG.CREATED_AT)
                .limit(safeSize)
                .offset((long) safePage * safeSize)
                .fetch();

        return Page.of(records.stream().map(LogMapper::toDto).toList(), safePage, safeSize, total);
    }
}
