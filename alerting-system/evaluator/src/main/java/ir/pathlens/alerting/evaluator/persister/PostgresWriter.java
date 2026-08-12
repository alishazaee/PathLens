package ir.pathlens.alerting.evaluator.persister;

import ir.pathlens.alerting.db.jooq.tables.Notification;
import ir.pathlens.alerting.db.jooq.tables.Rule;
import ir.pathlens.alerting.db.jooq.tables.records.NotificationRecord;
import ir.pathlens.alerting.db.jooq.tables.records.TrackedLogRecord;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

/** Persists tracked log records and notifications to PostgreSQL. */
public class PostgresWriter {
    private final DSLContext dsl;

    public PostgresWriter(DSLContext dsl) {
        this.dsl = dsl;
    }

    public void insertBatch(List<TrackedLogRecord> trackedLogs) {
        if (trackedLogs.isEmpty()) {
            return;
        }

        dsl.transaction(configuration -> {
            DSLContext ctx = DSL.using(configuration);

            Notification notification = Notification.NOTIFICATION;
            Rule rule = Rule.RULE;

            ctx.batchInsert(trackedLogs).execute();

            List<NotificationRecord> notifications = trackedLogs.stream()
                    .filter(TrackedLogRecord::getIsViolated)
                    .map(record -> {
                        NotificationRecord notificationRecord = new NotificationRecord();
                        notificationRecord.setRuleId(record.getRuleId());
                        notificationRecord.setLogTimestampHour(
                                record.getTimestamp().truncatedTo(ChronoUnit.HOURS)
                        );
                        return notificationRecord;
                    })
                    .toList();

            ctx.insertInto(notification)
                    .set(notifications)
                    .onConflict()
                    .doNothing()
                    .execute();

            Set<UUID> ruleIds = trackedLogs.stream()
                    .filter(TrackedLogRecord::getIsViolated)
                    .map(TrackedLogRecord::getRuleId)
                    .collect(Collectors.toSet());

            ctx.update(rule)
                    .set(rule.IS_VIOLATED, true)
                    .where(rule.ID.in(ruleIds))
                    .and(rule.IS_VIOLATED.eq(false))
                    .execute();
        });
    }
}
