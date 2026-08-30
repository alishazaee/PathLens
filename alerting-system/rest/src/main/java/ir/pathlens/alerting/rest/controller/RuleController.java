package ir.pathlens.alerting.rest.controller;

import ir.pathlens.alerting.model.ApiPathConstants;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleCreateDto;
import ir.pathlens.alerting.model.RuleFilter;
import ir.pathlens.alerting.model.RuleUpdateDto;
import ir.pathlens.alerting.rest.service.RuleService;
import ir.pathlens.common.model.Page;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
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
    public Rule createRule(@RequestBody @Valid RuleCreateDto rule) {
        return ruleService.createRule(rule);
    }

    @GetMapping(ApiPathConstants.GET_RULE_PATH)
    public Rule getRule(@PathVariable String id) {
        return ruleService.getRule(UUID.fromString(id));
    }

    @PutMapping(ApiPathConstants.UPDATE_RULE_PATH)
    public Rule updateRule(@PathVariable String id, @RequestBody @Valid RuleUpdateDto rule) {
        return ruleService.updateRule(UUID.fromString(id), rule);
    }

    @DeleteMapping(ApiPathConstants.DELETE_RULE_PATH)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteRule(@PathVariable String id) {
        ruleService.deleteRule(UUID.fromString(id));
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
    public Page<Rule> search(RuleFilter filter,
                             @RequestParam(defaultValue = "0") int page,
                             @RequestParam(defaultValue = "20") int size) {
        return ruleService.search(filter, page, size);
    }
}
