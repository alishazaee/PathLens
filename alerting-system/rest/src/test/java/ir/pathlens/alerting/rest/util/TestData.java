package ir.pathlens.alerting.rest.util;

import static ir.pathlens.alerting.db.jooq.tables.Notification.NOTIFICATION;
import static ir.pathlens.alerting.db.jooq.tables.Rule.RULE;
import static ir.pathlens.alerting.db.jooq.tables.TrackedLog.TRACKED_LOG;

import ir.pathlens.alerting.db.jooq.tables.records.NotificationRecord;
import ir.pathlens.alerting.db.jooq.tables.records.RuleRecord;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.RuleType;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.jooq.DSLContext;

/**
 * Test utilities for seeding data directly through jOOQ.
 */
public final class TestData {

    private TestData() {
    }

    public static UUID insertRule(DSLContext dsl, String title, String geometryWkt, LocalDateTime expiresAt,
                                  IdentityWrapper identity, RuleType ruleType, boolean isActive) {
        RuleRecord record = dsl.insertInto(RULE)
                .columns(RULE.TITLE, RULE.GEOMETRY_WKT, RULE.EXPIRES_AT, RULE.IDENTITY_TYPE,
                        RULE.IDENTITY_VALUE, RULE.RULE_TYPE, RULE.IS_ACTIVE)
                .values(title, geometryWkt, expiresAt, identity.identityType().name(),
                        identity.identityValue(), ruleType.name(), isActive)
                .returning(RULE.ID)
                .fetchOne();
        return record.getId();
    }

    public static UUID insertNotification(DSLContext dsl, UUID ruleId, boolean seen) {
        NotificationRecord record = dsl.insertInto(NOTIFICATION)
                .columns(NOTIFICATION.RULE_ID, NOTIFICATION.SEEN, NOTIFICATION.LOG_TIMESTAMP_HOUR)
                .values(ruleId, seen, LocalDateTime.now().truncatedTo(ChronoUnit.HOURS))
                .returning(NOTIFICATION.ID)
                .fetchOne();
        return record.getId();
    }

    public static void insertLog(DSLContext dsl, UUID ruleId, double latitude, double longitude,
                                 boolean violated) {
        dsl.insertInto(TRACKED_LOG)
                .columns(TRACKED_LOG.RULE_ID, TRACKED_LOG.LATITUDE, TRACKED_LOG.LONGITUDE,
                        TRACKED_LOG.IS_VIOLATED, TRACKED_LOG.TIMESTAMP)
                .values(ruleId, latitude, longitude, violated, LocalDateTime.now())
                .execute();
    }

    public static void clearAll(DSLContext dsl) {
        dsl.deleteFrom(TRACKED_LOG).execute();
        dsl.deleteFrom(NOTIFICATION).execute();
        dsl.deleteFrom(RULE).execute();
    }
}
