package ir.pathlens.alerting.rest.controllers;

import static ir.pathlens.alerting.model.ApiPathConstants.SEARCH_TARGET_LOGS_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.GeometryUtils;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.RuleCreateDto;
import ir.pathlens.alerting.model.RuleType;
import ir.pathlens.alerting.rest.entity.LogEntity;
import ir.pathlens.alerting.rest.entity.RuleEntity;
import ir.pathlens.alerting.rest.mappers.RuleMapper;
import ir.pathlens.alerting.rest.repository.LogRepository;
import ir.pathlens.alerting.rest.repository.RuleRepository;
import ir.pathlens.alerting.rest.util.CommonConfigs;
import ir.pathlens.extension.kafka.KafkaExtension;
import ir.pathlens.extension.postgresql.PostgresqlExtension;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith({PostgresqlExtension.class, KafkaExtension.class})
class LogControllerTest extends CommonConfigs {

    @Autowired
    private TestRestTemplate restTemplate;
    @Autowired
    private RuleRepository ruleRepository;
    @Autowired
    private LogRepository logRepository;

    private RuleEntity savedRule;

    @BeforeEach
    void setup() {
        logRepository.deleteAll();
        ruleRepository.deleteAll();
        restTemplate.getRestTemplate().setRequestFactory(new JdkClientHttpRequestFactory());
        savedRule = ruleRepository.save(RuleMapper.fromDto(
                new RuleCreateDto(
                        "test-rule",
                        GeometryUtils.createRandomWkt(),
                        LocalDateTime.now().plusDays(1).truncatedTo(ChronoUnit.SECONDS),
                        new IdentityWrapper(IdentityType.PhoneNumber, "09120000000"),
                        RuleType.Enter
                )));
    }

    @AfterEach
    void cleanUp() {
        logRepository.deleteAll();
        ruleRepository.deleteAll();
    }

    @Test
    void testSearchLogsWithoutFilter() {
        LogEntity violatedLog = new LogEntity();
        violatedLog.setRule(savedRule);
        violatedLog.setViolated(true);
        violatedLog.setLatitude(35.5);
        violatedLog.setLongitude(51.5);
        logRepository.save(violatedLog);

        LogEntity nonViolatedLog = new LogEntity();
        nonViolatedLog.setRule(savedRule);
        nonViolatedLog.setViolated(false);
        nonViolatedLog.setLatitude(36.0);
        nonViolatedLog.setLongitude(52.0);
        logRepository.save(nonViolatedLog);

        ResponseEntity<Map> response = restTemplate.getForEntity(
                SEARCH_TARGET_LOGS_PATH, Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        Map<String, Object> body = response.getBody();
        assertNotNull(body);
        assertEquals(2, body.get("totalElements"));
    }

    @Test
    void testSearchLogsWithViolationFilter() {
        LogEntity violatedLog = new LogEntity();
        violatedLog.setRule(savedRule);
        violatedLog.setViolated(true);
        violatedLog.setLatitude(35.5);
        violatedLog.setLongitude(51.5);
        logRepository.save(violatedLog);

        LogEntity nonViolatedLog = new LogEntity();
        nonViolatedLog.setRule(savedRule);
        nonViolatedLog.setViolated(false);
        nonViolatedLog.setLatitude(36.0);
        nonViolatedLog.setLongitude(52.0);
        logRepository.save(nonViolatedLog);

        String url = SEARCH_TARGET_LOGS_PATH + "?isViolated=true";
        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        Map<String, Object> body = response.getBody();
        assertNotNull(body);
        assertEquals(1, body.get("totalElements"));
        List<Map<String, Object>> content = (List<Map<String, Object>>) body.get("content");
        assertNotNull(content);
        assertEquals(1, content.size());
        Map<String, Object> log = content.get(0);
        assertTrue((Boolean) log.get("isViolated"));
        assertEquals(35.5, log.get("latitude"));
        assertEquals(51.5, log.get("longitude"));
        assertEquals(savedRule.getId().toString(), log.get("ruleId"));
    }

    @Test
    void testSearchLogsWithRuleIdFilter() {
        LogEntity violatedLog = new LogEntity();
        violatedLog.setRule(savedRule);
        violatedLog.setViolated(true);
        violatedLog.setLatitude(35.5);
        violatedLog.setLongitude(51.5);
        logRepository.save(violatedLog);

        String url = SEARCH_TARGET_LOGS_PATH + "?ruleId=" + savedRule.getId();
        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        Map<String, Object> body = response.getBody();
        assertNotNull(body);
        assertEquals(1, body.get("totalElements"));
        List<Map<String, Object>> content = (List<Map<String, Object>>) body.get("content");
        assertNotNull(content);
        assertEquals(1, content.size());
        Map<String, Object> log = content.get(0);
        assertEquals(savedRule.getId().toString(), log.get("ruleId"));
        assertTrue((Boolean) log.get("isViolated"));
    }
}
