package ir.pathlens.alerting.rest.controllers;

import static ir.pathlens.alerting.model.ApiPathConstants.SEARCH_TARGET_LOGS_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.GeometryUtils;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.RuleType;
import ir.pathlens.alerting.rest.util.ControllerTestBase;
import ir.pathlens.alerting.rest.util.TestData;
import ir.pathlens.extension.postgresql.PostgresqlExtension;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(PostgresqlExtension.class)
@SuppressWarnings("unchecked")
class LogControllerTest extends ControllerTestBase {

    @Autowired
    private TestRestTemplate restTemplate;
    @Autowired
    private DSLContext dsl;

    private UUID ruleId;

    @BeforeEach
    void setup() {
        TestData.clearAll(dsl);
        ruleId = TestData.insertRule(dsl, "test-rule", GeometryUtils.createRandomWkt(),
                LocalDateTime.now().plusDays(1).truncatedTo(ChronoUnit.SECONDS),
                new IdentityWrapper(IdentityType.PhoneNumber, "09120000000"), RuleType.Enter, true);
    }

    @AfterEach
    void cleanUp() {
        TestData.clearAll(dsl);
    }

    @Test
    void testSearchLogsWithoutFilter() {
        TestData.insertLog(dsl, ruleId, 35.5, 51.5, true);
        TestData.insertLog(dsl, ruleId, 36.0, 52.0, false);

        ResponseEntity<Map> response = restTemplate.getForEntity(SEARCH_TARGET_LOGS_PATH, Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        Map<String, Object> body = response.getBody();
        assertNotNull(body);
        assertEquals(2, body.get("totalElements"));
    }

    @Test
    void testSearchLogsWithViolationFilter() {
        TestData.insertLog(dsl, ruleId, 35.5, 51.5, true);
        TestData.insertLog(dsl, ruleId, 36.0, 52.0, false);

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
        assertEquals(ruleId.toString(), log.get("ruleId"));
        assertNotNull(log.get("timestamp"));
    }

    @Test
    void testSearchLogsWithPagination() {
        TestData.insertLog(dsl, ruleId, 35.5, 51.5, true);
        TestData.insertLog(dsl, ruleId, 36.0, 52.0, false);
        TestData.insertLog(dsl, ruleId, 37.0, 53.0, true);

        ResponseEntity<Map> firstPage = restTemplate.getForEntity(
                SEARCH_TARGET_LOGS_PATH + "?page=0&size=2", Map.class);
        assertEquals(HttpStatus.OK, firstPage.getStatusCode());
        Map<String, Object> firstBody = firstPage.getBody();
        assertNotNull(firstBody);
        assertEquals(3, firstBody.get("totalElements"));
        assertEquals(2, firstBody.get("totalPages"));
        assertEquals(0, firstBody.get("page"));
        assertEquals(2, ((List<Map<String, Object>>) firstBody.get("content")).size());

        ResponseEntity<Map> secondPage = restTemplate.getForEntity(
                SEARCH_TARGET_LOGS_PATH + "?page=1&size=2", Map.class);
        assertEquals(HttpStatus.OK, secondPage.getStatusCode());
        Map<String, Object> secondBody = secondPage.getBody();
        assertNotNull(secondBody);
        assertEquals(1, ((List<Map<String, Object>>) secondBody.get("content")).size());
    }

    @Test
    void testSearchLogsWithRuleIdFilter() {
        TestData.insertLog(dsl, ruleId, 35.5, 51.5, true);

        String url = SEARCH_TARGET_LOGS_PATH + "?ruleId=" + ruleId;
        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        Map<String, Object> body = response.getBody();
        assertNotNull(body);
        assertEquals(1, body.get("totalElements"));
        List<Map<String, Object>> content = (List<Map<String, Object>>) body.get("content");
        assertNotNull(content);
        assertEquals(1, content.size());
        Map<String, Object> log = content.get(0);
        assertEquals(ruleId.toString(), log.get("ruleId"));
        assertTrue((Boolean) log.get("isViolated"));
    }
}
