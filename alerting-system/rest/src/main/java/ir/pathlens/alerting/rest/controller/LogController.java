package ir.pathlens.alerting.rest.controller;

import ir.pathlens.alerting.model.ApiPathConstants;
import ir.pathlens.alerting.model.LogFilter;
import ir.pathlens.alerting.model.LogResponse;
import ir.pathlens.alerting.rest.service.LogService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Target log endpoints.
 */
@RestController
@RequiredArgsConstructor
public class LogController {

    private final LogService logSearchService;

    @GetMapping(ApiPathConstants.SEARCH_TARGET_LOGS_PATH)
    public Page<LogResponse> search(LogFilter filter, Pageable pageable) {
        return logSearchService.search(filter, pageable);
    }
}