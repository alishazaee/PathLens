package ir.pathlens.alerting.rest.mappers;

import ir.pathlens.alerting.db.jooq.tables.records.TrackedLogRecord;
import ir.pathlens.alerting.model.LogResponse;

/**
 * Mapper for target log models.
 */
public class LogMapper {

    public static LogResponse toDto(TrackedLogRecord log) {
        return new LogResponse(
                log.getId(),
                log.getIsViolated(),
                log.getRuleId(),
                log.getLatitude() == null ? 0.0 : log.getLatitude(),
                log.getLongitude() == null ? 0.0 : log.getLongitude(),
                log.getTimestamp()
        );
    }
}
