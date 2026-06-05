package ir.pathlens.alerting.rest.controller;

import ir.pathlens.alerting.model.ApiPathConstants;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.rest.service.RuleService;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal alerting endpoints.
 */
@RestController
public class InternalController {
    private final RuleService ruleService;

    public InternalController(RuleService ruleService) {
        this.ruleService = ruleService;
    }

    @GetMapping(ApiPathConstants.GET_ACTIVE_RULES_PATH)
    public List<Rule> getActiveRules() {
        return ruleService.getAllActiveRules();
    }

    @GetMapping(ApiPathConstants.GET_RULES_REVISION_PATH)
    public long getRevisionNumber() {
        return ruleService.getRevisionNumber();
    }
}
