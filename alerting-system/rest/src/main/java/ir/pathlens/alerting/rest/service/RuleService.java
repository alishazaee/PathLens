package ir.pathlens.alerting.rest.service;

import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleCreateDto;
import ir.pathlens.alerting.model.RuleFilter;
import ir.pathlens.alerting.rest.entity.RuleEntity;
import ir.pathlens.alerting.rest.filters.RuleSpecification;
import ir.pathlens.alerting.rest.mappers.RuleMapper;
import ir.pathlens.alerting.rest.repository.RuleRepository;
import jakarta.transaction.Transactional;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Service for rule operations.
 */
@Service
@Transactional
public class RuleService {
    private final RuleRepository ruleRepository;

    public RuleService(RuleRepository ruleRepository) {
        this.ruleRepository = ruleRepository;
    }

    public Rule createRule(RuleCreateDto rule) {
        RuleEntity ruleEntity = ruleRepository.save(RuleMapper.fromDto(rule));
        return RuleMapper.toDto(ruleEntity);
    }

    public Rule activateRule(UUID id) {
        RuleEntity ruleEntity = ruleRepository.findById(id).orElseThrow(
                () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Rule with id " + id + " not found"));
        ruleEntity.setActive(true);
        return RuleMapper.toDto(ruleEntity);
    }

    public Rule deactivateRule(UUID id) {
        RuleEntity ruleEntity = ruleRepository.findById(id).orElseThrow(
                () -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Rule with id " + id + " not found"));
        ruleEntity.setActive(false);
        return RuleMapper.toDto(ruleEntity);
    }

    public Page<Rule> search(RuleFilter filter, Pageable pageable) {
        Page<RuleEntity> page = ruleRepository.findAll(RuleSpecification.fromFilter(filter), pageable);
        return page.map(RuleMapper::toDto);
    }

    public List<Rule> getAllActiveRules() {
        return ruleRepository.findByIsActiveTrueAndExpiresAtAfter(LocalDateTime.now())
                .stream()
                .map(RuleMapper::toDto)
                .toList();
    }

    public long getRevisionNumber() {
        Optional<RuleEntity> ruleEntity = ruleRepository.findTopByOrderByUpdatedAtDesc();
        if (ruleEntity.isEmpty()) {
            return 0;
        }
        LocalDateTime localDateTime = ruleEntity.get().getUpdatedAt();
        ZoneId tehranZone = ZoneId.of("Asia/Tehran");
        ZoneOffset offset = tehranZone.getRules().getOffset(localDateTime);
        return localDateTime.toEpochSecond(offset);
    }

    public void updateRulesViolationToTrue(List<RuleEntity> rules) {
        rules.forEach(rule -> rule.setViolated(true));
        ruleRepository.saveAll(rules);
    }

    public List<RuleEntity> getActiveRuleEntitiesByIds(List<UUID> ruleIds) {
        return ruleRepository.findByIdInAndIsActiveTrueAndExpiresAtAfter(ruleIds, LocalDateTime.now());
    }

}
