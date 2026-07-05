package ir.pathlens.alerting.rest.controller;

import ir.pathlens.alerting.model.ApiPathConstants;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleCreateDto;
import ir.pathlens.alerting.model.RuleFilter;
import ir.pathlens.alerting.rest.service.RuleService;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for Rule service.
 */
@RestController
public class RuleController {

    private final RuleService ruleService;

    public RuleController(RuleService ruleService) {
        this.ruleService = ruleService;
    }

    @PostMapping(ApiPathConstants.CREATE_RULE_PATH)
    @ResponseStatus(HttpStatus.CREATED)
    public Rule createRule(@RequestBody RuleCreateDto rule) {
        return ruleService.createRule(rule);
    }

    @PatchMapping(ApiPathConstants.ACTIVATE_RULE_PATH)
    public Rule activateRule(@PathVariable String id) {
        return ruleService.activateRule(UUID.fromString(id));
    }

    @PatchMapping(ApiPathConstants.DEACTIVATE_RULE_PATH)
    public Rule deactivateRule(@PathVariable String id) {
        return ruleService.deactivateRule(UUID.fromString(id));
    }

    @GetMapping(ApiPathConstants.SEARCH_RULES_PATH)
    public Page<Rule> search(RuleFilter filter, Pageable pageable) {
        return ruleService.search(filter, pageable);
    }
}
