package ir.pathlens.alerting.rest.controller;

import ir.pathlens.alerting.model.ApiPathConstants;
import ir.pathlens.alerting.model.LogFilter;
import ir.pathlens.alerting.model.LogResponse;
import ir.pathlens.alerting.rest.service.LogService;
import ir.pathlens.common.model.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Target log endpoints.
 */
@RestController
@RequiredArgsConstructor
public class LogController {

    private final LogService logSearchService;

    @GetMapping(ApiPathConstants.SEARCH_TARGET_LOGS_PATH)
    public Page<LogResponse> search(LogFilter filter,
                                    @RequestParam(defaultValue = "0") int page,
                                    @RequestParam(defaultValue = "20") int size) {
        return logSearchService.search(filter, page, size);
    }
}