package ir.pathlens.alerting.rest.service;

import static ir.pathlens.extension.kafka.KafkaExtension.getKafkaContainer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.GeometryUtils;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.LogResponse;
import ir.pathlens.alerting.model.Notification;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleCreateDto;
import ir.pathlens.alerting.model.RuleType;
import ir.pathlens.alerting.rest.configs.ApplicationConfig;
import ir.pathlens.alerting.rest.repository.LogRepository;
import ir.pathlens.alerting.rest.repository.NotificationRepository;
import ir.pathlens.alerting.rest.repository.RuleRepository;
import ir.pathlens.alerting.rest.util.TargetLogRandomGenerator;
import ir.pathlens.extension.kafka.KafkaExtension;
import ir.pathlens.extension.postgresql.PostgresqlExtension;
import ir.pathlens.extension.postgresql.SpringCommonPostgresConfigs;
import ir.pathlens.proto.TargetLogProto.TargetLog;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@SpringBootTest
@ExtendWith({PostgresqlExtension.class, KafkaExtension.class})
class TargetLogBatchPersisterTest extends SpringCommonPostgresConfigs {
    private static UUID ENTER_VIOLATION_RULE_ID;
    private static UUID EXIT_VIOLATION_RULE_ID;
    private static UUID EXPIRED_RULE_ID;

    private final IdentityWrapper sampleIdentity = new IdentityWrapper(IdentityType.PhoneNumber, "09124505456");
    private final String randomGeometry = GeometryUtils.createRandomWkt();
    private KafkaProducer<byte[], byte[]> producer;
    @Autowired
    private RuleService ruleService;
    @Autowired
    private LogService logService;
    @Autowired
    private NotificationService notificationService;
    @Autowired
    private RuleRepository ruleRepository;
    @Autowired
    private LogRepository logRepository;
    @Autowired
    private NotificationRepository notificationRepository;
    @Autowired
    private ApplicationConfig config;

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("app.target-log-consumer.bootstrap-servers", getKafkaContainer()::getBootstrapServers);
    }

    @BeforeEach
    void setup() {
        producer = createKafkaProducer();
        RuleCreateDto enterRule = new RuleCreateDto("rule-1", randomGeometry, LocalDateTime.now().plusDays(2),
                sampleIdentity, RuleType.Enter);
        Rule enterViolation = ruleService.createRule(enterRule);
        ENTER_VIOLATION_RULE_ID = enterViolation.id();
        RuleCreateDto exitRule = new RuleCreateDto("rule-2", randomGeometry, LocalDateTime.now().plusHours(1),
                sampleIdentity, RuleType.Exit);
        Rule exitViolation = ruleService.createRule(exitRule);
        EXIT_VIOLATION_RULE_ID = exitViolation.id();
        RuleCreateDto expiredRule = new RuleCreateDto("rule-3", randomGeometry, LocalDateTime.now().minusHours(1),
                sampleIdentity, RuleType.Enter);
        Rule expiredViolation = ruleService.createRule(expiredRule);
        EXPIRED_RULE_ID = expiredViolation.id();
    }

    @AfterEach
    void cleanUp() {
        notificationRepository.deleteAll();
        logRepository.deleteAll();
        ruleRepository.deleteAll();
    }

    @Test
    void shouldPersistEnterViolationLogs() throws Exception {
        List<TargetLog> expectedTargetLogs = generateTargetLogs(ENTER_VIOLATION_RULE_ID, RuleType.Enter, 100, 200);
        publishTargetLogs(expectedTargetLogs);
        Awaitility.await().atMost(1, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> {
                    Page<LogResponse> persistedLogs = getPersistedTargetLogs();
                    return (expectedTargetLogs.size() == persistedLogs.getTotalElements());
                });
        assertPersistedLogsMatch(expectedTargetLogs);
        assertPersistedNotificationMatch(ENTER_VIOLATION_RULE_ID);
    }

    @Test
    void shouldPersistExitViolationLogs() throws Exception {
        List<TargetLog> expectedTargetLogs = generateTargetLogs(EXIT_VIOLATION_RULE_ID, RuleType.Exit, 100, 200);
        publishTargetLogs(expectedTargetLogs);
        Awaitility.await().atMost(1, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> {
                    Page<LogResponse> persistedLogs = getPersistedTargetLogs();
                    return (expectedTargetLogs.size() == persistedLogs.getTotalElements());
                });
        assertPersistedLogsMatch(expectedTargetLogs);
        assertPersistedNotificationMatch(EXIT_VIOLATION_RULE_ID);
    }

    @Test
    void shouldIgnoreExpiredRuleLogs() throws Exception {
        List<TargetLog> expiredLogs =
                generateTargetLogs(EXPIRED_RULE_ID, RuleType.Enter, 500, 300);
        List<TargetLog> validLogs =
                generateTargetLogs(ENTER_VIOLATION_RULE_ID, RuleType.Enter, 55, 45);

        publishTargetLogs(expiredLogs);
        publishTargetLogs(validLogs);

        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() ->
                        getPersistedTargetLogs().getTotalElements() == validLogs.size());

        assertPersistedLogsMatch(validLogs);
        assertPersistedNotificationMatch(ENTER_VIOLATION_RULE_ID);
    }

    private List<TargetLog> generateTargetLogs(UUID ruleId, RuleType ruleType, int insideCount, int outsideCount)
            throws Exception {
        return new TargetLogRandomGenerator(ruleId, ruleType, randomGeometry)
                .getRandomTargets(insideCount, outsideCount);
    }

    private void publishTargetLogs(List<TargetLog> targetLogs) {
        targetLogs.forEach(targetLog -> {
            ProducerRecord<byte[], byte[]> producerRecord =
                    new ProducerRecord<>(config.getTargetLogsSourceTopic(), null, targetLog.toByteArray());
            producer.send(producerRecord);
        });
        producer.flush();
    }

    private void assertPersistedLogsMatch(List<TargetLog> expectedLogs) {
        Page<LogResponse> persistedLogs = getPersistedTargetLogs();
        expectedLogs.forEach(expected -> assertTrue(
                        persistedLogs.stream().anyMatch(actual -> matches(actual, expected)),
                        "Expected log was not found: " + expected
                )
        );
    }

    private void assertPersistedNotificationMatch(UUID ruleId) {
        Pageable page = PageRequest.of(0, Integer.MAX_VALUE);
        Page<Notification> notifications = notificationService.search(null, page);
        assertEquals(1, notifications.getTotalElements());
        assertTrue(notifications.stream().anyMatch(notification -> notification.ruleId().equals(ruleId)));
    }

    private Page<LogResponse> getPersistedTargetLogs() {
        Pageable pageable = PageRequest.of(0, Integer.MAX_VALUE);
        return logService.search(null, pageable);
    }

    private boolean matches(LogResponse actual, TargetLog expected) {
        return actual.latitude() == expected.getLocation().getLatitude()
                && actual.longitude() == expected.getLocation().getLongitude()
                && actual.isViolated() == expected.getViolated()
                && Objects.equals(actual.ruleId().toString(), expected.getRuleId());
    }

    private KafkaProducer<byte[], byte[]> createKafkaProducer() {
        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaContainer().getBootstrapServers());
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(producerProp);
    }
}