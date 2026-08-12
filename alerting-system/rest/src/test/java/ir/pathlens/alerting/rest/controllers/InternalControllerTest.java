package ir.pathlens.alerting.rest.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.GeometryUtils;
import ir.pathlens.alerting.client.RulesClient;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleType;
import ir.pathlens.alerting.rest.util.ControllerTestBase;
import ir.pathlens.alerting.rest.util.TestData;
import ir.pathlens.client.ApiCallException;
import ir.pathlens.extension.postgresql.PostgresqlExtension;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(PostgresqlExtension.class)
class InternalControllerTest extends ControllerTestBase {

    private static final IdentityWrapper PHONE_IDENTITY =
            new IdentityWrapper(IdentityType.PhoneNumber, "09124505456");

    @Autowired
    private DSLContext dsl;
    @LocalServerPort
    private int serverPort;

    private RulesClient rulesClient;

    @BeforeEach
    void setUp() {
        TestData.clearAll(dsl);
        rulesClient = new RulesClient("http://127.0.0.1:" + serverPort);
    }

    @AfterEach
    void tearDown() {
        TestData.clearAll(dsl);
        rulesClient.close();
    }

    @Test
    void testGetActiveRules() throws ApiCallException {
        TestData.insertRule(dsl, "inactive-rule", GeometryUtils.createRandomWkt(),
                LocalDateTime.now().plusDays(1).truncatedTo(ChronoUnit.SECONDS), PHONE_IDENTITY, RuleType.Enter, false);
        TestData.insertRule(dsl, "expired-rule", GeometryUtils.createRandomWkt(),
                LocalDateTime.now().minusDays(1).truncatedTo(ChronoUnit.SECONDS), PHONE_IDENTITY, RuleType.Exit, true);
        UUID activeRule = TestData.insertRule(dsl, "active-rule", GeometryUtils.createRandomWkt(),
                LocalDateTime.now().plusDays(1).truncatedTo(ChronoUnit.SECONDS), PHONE_IDENTITY, RuleType.Enter, true);

        List<Rule> rules = rulesClient.getAllActiveRules();
        assertEquals(1, rules.size());
        assertEquals(activeRule, rules.get(0).id());
    }

    @Test
    void testGetRevisionNumber() throws ApiCallException {
        assertEquals(0, rulesClient.getRevisionNumber());

        TestData.insertRule(dsl, "active-rule", GeometryUtils.createRandomWkt(),
                LocalDateTime.now().plusDays(1).truncatedTo(ChronoUnit.SECONDS), PHONE_IDENTITY, RuleType.Enter, true);

        assertTrue(rulesClient.getRevisionNumber() > 0);
    }
}
