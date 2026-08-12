package ir.pathlens.alerting.rest.controllers;

import static ir.pathlens.alerting.model.ApiPathConstants.GET_NOTIFICATION_PATH;
import static ir.pathlens.alerting.model.ApiPathConstants.SEARCH_NOTIFICATIONS_PATH;
import static ir.pathlens.alerting.model.ApiPathConstants.SET_NOTIFICATION_SEEN_PATH;
import static ir.pathlens.alerting.model.ApiPathConstants.buildPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.GeometryUtils;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.Notification;
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
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(PostgresqlExtension.class)
@SuppressWarnings("unchecked")
class NotificationControllerTest extends ControllerTestBase {

    @Autowired
    private TestRestTemplate restTemplate;
    @Autowired
    private DSLContext dsl;

    private UUID ruleId;
    private UUID notificationId;

    @BeforeEach
    void setup() {
        TestData.clearAll(dsl);
        restTemplate.getRestTemplate().setRequestFactory(new JdkClientHttpRequestFactory());
        ruleId = TestData.insertRule(dsl, "test-rule", GeometryUtils.createRandomWkt(),
                LocalDateTime.now().plusDays(1).truncatedTo(ChronoUnit.SECONDS),
                new IdentityWrapper(IdentityType.PhoneNumber, "09120000000"), RuleType.Enter, true);
        notificationId = TestData.insertNotification(dsl, ruleId, false);
    }

    @AfterEach
    void cleanUp() {
        TestData.clearAll(dsl);
    }

    @Test
    void testGetNotification() {
        String url = buildPath(GET_NOTIFICATION_PATH, notificationId.toString());
        ResponseEntity<Notification> response = restTemplate.getForEntity(url, Notification.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        Notification body = response.getBody();
        assertNotNull(body);
        assertEquals(notificationId, body.id());
        assertNotNull(body.createdAt());
        assertEquals("PHONE NUMBER 09120000000 has entered the zone", body.message());
        assertEquals(ruleId, body.ruleId());
        assertFalse(body.seen());
        assertTrue(body.isActive());
    }

    @Test
    void testGetNotificationNotFound() {
        String url = buildPath(GET_NOTIFICATION_PATH, UUID.randomUUID().toString());
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    @Test
    void testSetNotificationSeen() {
        String url = buildPath(SET_NOTIFICATION_SEEN_PATH, notificationId.toString());
        ResponseEntity<Notification> response = restTemplate.exchange(
                url, HttpMethod.PATCH, null, Notification.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        Notification body = response.getBody();
        assertNotNull(body);
        assertTrue(body.seen());
    }

    @Test
    void testSearchNotifications() {
        ResponseEntity<Map> response = restTemplate.getForEntity(SEARCH_NOTIFICATIONS_PATH, Map.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        Map<String, Object> body = response.getBody();
        assertNotNull(body);
        assertEquals(1, body.get("totalElements"));
        List<Map<String, Object>> content = (List<Map<String, Object>>) body.get("content");
        assertNotNull(content);
        assertEquals(1, content.size());
        Map<String, Object> notification = content.get(0);
        assertEquals(notificationId.toString(), notification.get("id"));
        assertEquals(ruleId.toString(), notification.get("ruleId"));
        assertEquals("PHONE NUMBER 09120000000 has entered the zone", notification.get("message"));
        assertFalse((Boolean) notification.get("seen"));
        assertTrue((Boolean) notification.get("isActive"));
    }

    @Test
    void testSearchNotificationsWithFilter() {
        ResponseEntity<Map> unseen = restTemplate.getForEntity(
                SEARCH_NOTIFICATIONS_PATH + "?seen=false", Map.class);
        assertEquals(HttpStatus.OK, unseen.getStatusCode());
        assertEquals(1, unseen.getBody().get("totalElements"));

        ResponseEntity<Map> seen = restTemplate.getForEntity(
                SEARCH_NOTIFICATIONS_PATH + "?seen=true", Map.class);
        assertEquals(HttpStatus.OK, seen.getStatusCode());
        assertEquals(0, seen.getBody().get("totalElements"));
    }
}
