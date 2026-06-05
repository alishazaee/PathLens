package ir.pathlens.alerting.rest.service;

import ir.pathlens.alerting.model.LogFilter;
import ir.pathlens.alerting.model.LogResponse;
import ir.pathlens.alerting.rest.entity.LogEntity;
import ir.pathlens.alerting.rest.entity.RuleEntity;
import ir.pathlens.alerting.rest.filters.LogSpecification;
import ir.pathlens.alerting.rest.mappers.LogMapper;
import ir.pathlens.alerting.rest.repository.LogRepository;
import ir.pathlens.proto.TargetLogProto.TargetLog;
import jakarta.transaction.Transactional;
import java.util.List;
import java.util.Map;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

/**
 * Service for target log operations.
 */
@Service
@Transactional
public class LogService {

    private final LogRepository logRepository;

    public LogService(LogRepository logRepository) {
        this.logRepository = logRepository;
    }

    public Page<LogResponse> search(LogFilter filter, Pageable pageable) {
        return logRepository.findAll(
                LogSpecification.withFilter(filter),
                pageable
        ).map(LogMapper::toDto);
    }

    public void createLogs(Map<TargetLog, RuleEntity> targetLogRuleMap) {

        List<LogEntity> logEntities = targetLogRuleMap.entrySet()
                .stream()
                .map(entry -> {
                    TargetLog targetLog = entry.getKey();
                    RuleEntity rule = entry.getValue();

                    LogEntity logEntity = new LogEntity();
                    logEntity.setRule(rule);
                    logEntity.setViolated(targetLog.getViolated());
                    logEntity.setLatitude(targetLog.getLocation().getLatitude());
                    logEntity.setLongitude(targetLog.getLocation().getLongitude());

                    return logEntity;
                })
                .toList();

        logRepository.saveAll(logEntities);
    }
}
