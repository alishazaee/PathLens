package ir.pathlens.alerting.rest.mappers;

import ir.pathlens.alerting.model.LogResponse;
import ir.pathlens.alerting.rest.entity.LogEntity;

/**
 * Mapper for target log models.
 */
public class LogMapper {

    public static LogResponse toDto(LogEntity log) {
        return new LogResponse(
                log.getId(),
                log.isViolated(),
                log.getRule().getId(),
                log.getLatitude(),
                log.getLongitude()
        );
    }
}
