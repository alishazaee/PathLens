package ir.pathlens.alerting.rest.service;

import ir.pathlens.alerting.rest.entity.RuleEntity;
import ir.pathlens.proto.TargetLogProto.TargetLog;
import jakarta.transaction.Transactional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Persists target logs in batches.
 */
@Service
@Transactional
public class TargetLogBatchPersister {

    private static final Logger logger =
            LoggerFactory.getLogger(TargetLogBatchPersister.class);

    private final RuleService ruleService;
    private final NotificationService notificationService;
    private final LogService logService;

    public TargetLogBatchPersister(
            RuleService ruleService,
            LogService logService,
            NotificationService notificationService) {
        this.ruleService = ruleService;
        this.logService = logService;
        this.notificationService = notificationService;
    }

    public void processBatch(List<TargetLog> logs) {

        List<UUID> ruleIds = logs.stream()
                .map(log -> UUID.fromString(log.getRuleId()))
                .distinct()
                .toList();

        Map<UUID, RuleEntity> rulesById = ruleService
                .getActiveRuleEntitiesByIds(ruleIds)
                .stream()
                .collect(Collectors.toMap(
                        RuleEntity::getId,
                        Function.identity()
                ));

        Set<RuleEntity> violatedRules = new HashSet<>();
        Map<TargetLog, RuleEntity> targetLogsToSaveMap = new HashMap<>();

        for (TargetLog target : logs) {
            UUID ruleId = UUID.fromString(target.getRuleId());
            RuleEntity rule = rulesById.get(ruleId);

            if (rule == null) {
                continue;
            }

            if (target.getShouldNotify()) {
                violatedRules.add(rule);
            }
            targetLogsToSaveMap.put(target, rule);
        }

        if (!violatedRules.isEmpty()) {
            ruleService.updateRulesViolationToTrue(new ArrayList<>(violatedRules));

            for (RuleEntity rule : violatedRules) {
                notificationService.createNewNotification(rule);
            }
        }
        logService.createLogs(targetLogsToSaveMap);

        logger.info("Successfully persisted {} logs into database", logs.size());
    }
}