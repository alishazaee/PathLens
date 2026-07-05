package ir.pathlens.alerting.rest.controllers;

import static ir.pathlens.alerting.model.ApiPathConstants.ACTIVATE_RULE_PATH;
import static ir.pathlens.alerting.model.ApiPathConstants.CREATE_RULE_PATH;
import static ir.pathlens.alerting.model.ApiPathConstants.DEACTIVATE_RULE_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.GeometryUtils;
import ir.pathlens.alerting.client.RulesCache;
import ir.pathlens.alerting.client.RulesClient;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleCreateDto;
import ir.pathlens.alerting.model.RuleType;
import ir.pathlens.alerting.rest.repository.LogRepository;
import ir.pathlens.alerting.rest.repository.NotificationRepository;
import ir.pathlens.alerting.rest.repository.RuleRepository;
import ir.pathlens.alerting.rest.util.CommonConfigs;
import ir.pathlens.client.ApiCallException;
import ir.pathlens.extension.kafka.KafkaExtension;
import ir.pathlens.extension.postgresql.PostgresqlExtension;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith({PostgresqlExtension.class, KafkaExtension.class})
class RuleControllerTest extends CommonConfigs {

    private final LocalDateTime expiresAt = LocalDateTime.now().plusDays(2).truncatedTo(ChronoUnit.SECONDS);
    private final IdentityWrapper phoneIdentity = new IdentityWrapper(IdentityType.PhoneNumber, "09124505456");
    private final IdentityWrapper plateIdentity = new IdentityWrapper(IdentityType.PlateNumber, "123-j-12");
    private final String phoneWktGeometry = GeometryUtils.createRandomWkt();
    private final String plateWktGeometry = GeometryUtils.createRandomWkt();
    @Autowired
    private TestRestTemplate restTemplate;
    @Autowired
    private RuleRepository ruleRepository;
    @Autowired
    private LogRepository logRepository;
    @Autowired
    private NotificationRepository notificationRepository;
    @LocalServerPort
    private int serverPort;

    private RulesClient rulesClient;

    @BeforeEach
    void setup() {
        notificationRepository.deleteAll();
        ruleRepository.deleteAll();
        rulesClient = new RulesClient("http://127.0.0.1:" + serverPort);
        restTemplate.getRestTemplate().setRequestFactory(new JdkClientHttpRequestFactory());
    }

    @AfterEach
    void cleanUp() {
        notificationRepository.deleteAll();
        logRepository.deleteAll();
        ruleRepository.deleteAll();
        rulesClient.close();
    }

    @Test
    public void testCreateRule() throws ApiCallException {
        ResponseEntity<Rule> phoneRule = createNewRule(phoneWktGeometry, phoneIdentity, RuleType.Enter);
        assertRuleCreatedSuccessfully(
                constructExpectedRule(phoneWktGeometry, phoneIdentity, RuleType.Enter), phoneRule);
        ResponseEntity<Rule> plateRule = createNewRule(plateWktGeometry, plateIdentity, RuleType.Exit);
        assertRuleCreatedSuccessfully(
                constructExpectedRule(plateWktGeometry, plateIdentity, RuleType.Exit), plateRule);
        List<Rule> activeRules = rulesClient.getAllActiveRules();
        assertEquals(2, activeRules.size());
        assertTrue(activeRules.contains(phoneRule.getBody()));
        assertTrue(activeRules.contains(plateRule.getBody()));
    }

    @Test
    public void testCacheContainsCreatedRules() {
        ResponseEntity<Rule> phoneRule = createNewRule(phoneWktGeometry, phoneIdentity, RuleType.Enter);
        ResponseEntity<Rule> plateRule = createNewRule(plateWktGeometry, plateIdentity, RuleType.Exit);
        try (RulesCache cache = new RulesCache(rulesClient, 10, 100)) {
            cache.submitBackgroundTask();
            assertCacheContainsBothRules(cache, phoneRule, plateRule);
        }
    }

    @Test
    public void testIdenticalIdentitiesInCache() {
        ResponseEntity<Rule> phoneRuleEnter = createNewRule(phoneWktGeometry, phoneIdentity, RuleType.Enter);
        ResponseEntity<Rule> phoneRuleExit = createNewRule(phoneWktGeometry, phoneIdentity, RuleType.Exit);
        try (RulesCache cache = new RulesCache(rulesClient, 10, 100)) {{
            cache.submitBackgroundTask();
            assertCacheContainsBothIdentities(cache, phoneRuleEnter, phoneRuleExit);
        }}
    }

    @Test
    public void testRevisionNumber() throws ApiCallException {
        long revisionBefore = rulesClient.getRevisionNumber();
        assertEquals(0, revisionBefore);
        ResponseEntity<Rule> phoneRule = createNewRule(phoneWktGeometry, phoneIdentity, RuleType.Enter);
        assertEquals(HttpStatus.CREATED, phoneRule.getStatusCode());
        long revisionAfter = rulesClient.getRevisionNumber();

        ZoneId tehranZone = ZoneId.of("Asia/Tehran");
        ZoneOffset offset = tehranZone.getRules().getOffset(phoneRule.getBody().createdAt());
        assertEquals(phoneRule.getBody().createdAt().toEpochSecond(offset), revisionAfter);
    }

    @Test
    public void testRuleActivation() {
        ResponseEntity<Rule> phoneRule = createNewRule(phoneWktGeometry, phoneIdentity, RuleType.Enter);
        assertTrue(phoneRule.getBody().isActive());
        Rule response;

        response = restTemplate.patchForObject(DEACTIVATE_RULE_PATH, null, Rule.class, phoneRule.getBody().id());
        assertFalse(response.isActive());

        response = restTemplate.patchForObject(ACTIVATE_RULE_PATH, null, Rule.class, phoneRule.getBody().id());
        assertTrue(response.isActive());
    }

    private ResponseEntity<Rule> createNewRule(String wktGeometry, IdentityWrapper identity, RuleType type) {
        RuleCreateDto rule = new RuleCreateDto(null, wktGeometry, expiresAt, identity, type);
        return restTemplate.postForEntity(CREATE_RULE_PATH, rule, Rule.class);
    }

    private void assertRuleCreatedSuccessfully(Rule expected, ResponseEntity<Rule> response) {
        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        Rule body = response.getBody();
        assertNotNull(body);
        assertNotNull(body.id());

        assertEquals(expected.title(), body.title());
        assertEquals(expected.geometryWkt(), body.geometryWkt());
        assertEquals(expected.expiresAt(), body.expiresAt());
        assertEquals(expected.identity(), body.identity());
        assertEquals(expected.isActive(), body.isActive());
        assertEquals(expected.ruleType(), body.ruleType());
        assertEquals(expected.isViolated(), body.isViolated());
    }

    private Rule constructExpectedRule(String wktGeometry, IdentityWrapper identity, RuleType type) {
        return new Rule(null, "NO NAME", wktGeometry, expiresAt, identity, true, type, false, null);
    }

    private void assertCacheContainsBothRules(
            RulesCache cache, ResponseEntity<Rule> phoneRule, ResponseEntity<Rule> plateRule) {
        Awaitility.await().atMost(1, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> cache.snapshot().rulesCache().size() == 2);
        Rule phoneRuleBody = phoneRule.getBody();
        Rule plateRuleBody = plateRule.getBody();
        Set<UUID> plateRuleIds = cache.snapshot().getRulesIdsByIdentity(
                plateRuleBody.identity()).get();
        assertTrue(plateRuleIds.contains(plateRuleBody.id()));
        Set<UUID> phoneRuleIds = cache.snapshot().getRulesIdsByIdentity(
                phoneRuleBody.identity()).get();
        assertTrue(phoneRuleIds.contains(phoneRuleBody.id()));
        assertEquals(cache.snapshot().getEnterIntoRegionRuleGeometryWktByRuleId(
                phoneRuleBody.id()).get(), phoneRuleBody.geometryWkt());
        assertEquals(cache.snapshot().getLeavingRegionRuleGeometryWktByRuleId(
                plateRuleBody.id()).get(), plateRuleBody.geometryWkt());
    }

    private void assertCacheContainsBothIdentities(
            RulesCache cache, ResponseEntity<Rule> phoneRuleEnter, ResponseEntity<Rule> phoneRuleExit) {
        Awaitility.await().atMost(1, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> cache.snapshot().rulesCache().size() == 1);
        UUID phoneRuleEnterId = phoneRuleEnter.getBody().id();
        UUID phoneRuleExitId = phoneRuleExit.getBody().id();
        Set<UUID> expectedIds = Set.of(phoneRuleEnterId, phoneRuleExitId);
        assertEquals(expectedIds, cache.snapshot().rulesCache().get(phoneIdentity));
    }
}
