package ir.pathlens.alerting.evaluator;

import static ir.pathlens.GeometryUtils.createRandomWkt;
import static ir.pathlens.GeometryUtils.getRandomPointInsideWkt;
import static ir.pathlens.GeometryUtils.getRandomPointOutsideWkt;
import static ir.pathlens.alerting.evaluator.Evaluator.createDataSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import ir.pathlens.GeometryUtils.LatLon;
import ir.pathlens.alerting.db.jooq.tables.Notification;
import ir.pathlens.alerting.db.jooq.tables.Rule;
import ir.pathlens.alerting.db.jooq.tables.TrackedLog;
import ir.pathlens.alerting.evaluator.configs.ApplicationConfig;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.RuleType;
import ir.pathlens.alerting.rest.controller.MockRuleController;
import ir.pathlens.extension.kafka.KafkaExtension;
import ir.pathlens.extension.postgresql.PostgresqlExtension;
import ir.pathlens.generator.CameraLogGenerator;
import ir.pathlens.proto.CameraLogProto.Location;
import ir.pathlens.proto.CameraLogProto.Log;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@ExtendWith({KafkaExtension.class, PostgresqlExtension.class})
class EvaluatorTest {

    private MockRuleController mockController;
    private ApplicationConfig applicationConfig;
    private MeterRegistry meterRegistry;
    private Producer<byte[], byte[]> producer;
    private Evaluator evaluator;
    private DSLContext dsl;
    private HikariDataSource dataSource;

    private final KafkaContainer kafkaExtensionContainer = KafkaExtension.getKafkaContainer();
    private final PostgreSQLContainer<?> postgresqlContainer = PostgresqlExtension.getPostgresqlContainer();

    @BeforeEach
    void setup() throws IOException {
        applicationConfig = ConfigReader.loadConfig(Path.of("src/test/resources/application.yml"));
        mockController = new MockRuleController();

        applicationConfig.rulesCacheConfig().setBaseUrl(mockController.getBaseUrl());
        applicationConfig.postgresConfig().setUrl(postgresqlContainer.getJdbcUrl());
        applicationConfig.postgresConfig().setUsername(postgresqlContainer.getUsername());
        applicationConfig.postgresConfig().setPassword(postgresqlContainer.getPassword());
        EvaluatorMain.migrate(applicationConfig.postgresConfig());

        String bootstrapServers = kafkaExtensionContainer.getBootstrapServers();
        applicationConfig.kafkaProducerConfig().setBootstrapServers(bootstrapServers);
        applicationConfig.kafkaConsumerConfig().setBootstrapServers(bootstrapServers);
        meterRegistry =  new SimpleMeterRegistry();
        evaluator = new Evaluator(applicationConfig, meterRegistry);
        producer = createProducer();
        dataSource  = createDataSource(applicationConfig.postgresConfig());
        dsl = DSL.using(dataSource, SQLDialect.POSTGRES);
        clearTables();
    }

    private void clearTables() {
        dsl.deleteFrom(TrackedLog.TRACKED_LOG).execute();
        dsl.deleteFrom(Notification.NOTIFICATION).execute();
        dsl.deleteFrom(Rule.RULE).execute();
    }

    @AfterEach
    void tearDown() throws Exception {
        evaluator.close();
        if (producer != null) {
            producer.close();
        }
        mockController.close();
        dataSource.close();
    }

    @Test
    void testRuleMatchingDetection() throws Exception {
        UUID ruleId = UUID.randomUUID();
        IdentityWrapper identity = new IdentityWrapper(IdentityType.PlateNumber, "IR12AB345");
        String geometry = createRandomWkt();
        mockController.addRule(ruleId, "Test Rule", geometry, LocalDateTime.now().plusDays(1),
                identity, RuleType.Enter);
        insertRule(ruleId, "Test Rule", geometry, LocalDateTime.now().plusDays(1), identity, RuleType.Enter);
        evaluator.start();

        List<Log> logs = getRandomNoisyLogs();

        Log log1 = generateRandomLogAtGeometryPosition(geometry, GeometryPosition.INSIDE)
                .setTimestamp(getSampleTimestamp().getEpochSecond() * 1000)
                .setPlateNumber(identity.identityValue())
                .build();
        Log log2 = generateRandomLogAtGeometryPosition(geometry, GeometryPosition.OUTSIDE)
                .setTimestamp(getSampleTimestamp().plus(1, ChronoUnit.DAYS).getEpochSecond() * 1000)
                .setPlateNumber(identity.identityValue())
                .build();
        Log log3 = generateRandomLogAtGeometryPosition(geometry, GeometryPosition.INSIDE)
                .setTimestamp(getSampleTimestamp().plus(2, ChronoUnit.DAYS).getEpochSecond() * 1000)
                .setPlateNumber(identity.identityValue())
                .build();
        Log log4 = generateRandomLogAtGeometryPosition(geometry, GeometryPosition.OUTSIDE)
                .setTimestamp(getSampleTimestamp().plus(3, ChronoUnit.DAYS).getEpochSecond() * 1000)
                .setPlateNumber(identity.identityValue())
                .build();
        logs.addAll(List.of(log1, log2, log3, log4));
        Collections.shuffle(logs);

        for (Log log : logs) {
            produceLog(applicationConfig.sourceTopic(), log.toByteArray());
        }

        Awaitility.await()
                .pollInterval(Duration.ofMillis(1))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    int count = dsl.fetchCount(TrackedLog.TRACKED_LOG);
                    assertEquals(4, count);
                });

        assertTrue(isTrackedRecordPersisted(log1, ruleId, true));
        assertTrue(isTrackedRecordPersisted(log2, ruleId, false));
        assertTrue(isTrackedRecordPersisted(log3, ruleId, true));
        assertTrue(isTrackedRecordPersisted(log4, ruleId, false));
        assertEquals(2, getNotificationCount(ruleId));
        assertTrue(isRuleViolated(ruleId));

        assertEquals(4, meterRegistry.get(Profiler.MATCHED_LOGS_METRIC_COUNTER_NAME).counter().count());
        assertEquals(2, meterRegistry.get(Profiler.NON_VIOLATED_LOGS_METRIC_COUNTER_NAME).counter().count());
        assertEquals(2, meterRegistry.get(Profiler.VIOLATED_LOGS_METRIC_COUNTER_NAME).counter().count());
    }

    @Test
    void testLogWithMultipleRuleMatch() throws Exception {
        UUID rule1 = UUID.randomUUID();
        UUID rule2 = UUID.randomUUID();
        IdentityWrapper identity1 = new IdentityWrapper(IdentityType.PlateNumber, "IR12AB345");
        IdentityWrapper identity2 = new IdentityWrapper(IdentityType.PhoneNumber, "989124349853");

        String geometry = createRandomWkt();
        mockController.addRule(rule1, "Test Rule", geometry,
                LocalDateTime.now().plusDays(1), identity1, RuleType.Enter);
        mockController.addRule(rule2, "Test Rule", geometry,
                LocalDateTime.now().plusDays(1), identity2, RuleType.Enter);
        insertRule(rule1, "Test Rule", geometry, LocalDateTime.now().plusDays(1), identity1, RuleType.Enter);
        insertRule(rule2, "Test Rule", geometry, LocalDateTime.now().plusDays(1), identity2, RuleType.Enter);

        evaluator.start();
        List<Log> logs = getRandomNoisyLogs();
        Log violatedLog = generateRandomLogAtGeometryPosition(geometry, GeometryPosition.INSIDE)
                .setPlateNumber(identity1.identityValue())
                .setPhoneNumber(identity2.identityValue())
                .build();
        logs.add(violatedLog);
        Collections.shuffle(logs);

        for (Log log : logs) {
            produceLog(applicationConfig.sourceTopic(), log.toByteArray());
        }

        Awaitility.await()
                .pollInterval(Duration.ofMillis(1))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    int count = dsl.fetchCount(TrackedLog.TRACKED_LOG);
                    assertEquals(2, count);
                });

        assertTrue(isTrackedRecordPersisted(violatedLog, rule1, true));
        assertEquals(1, getNotificationCount(rule1));
        assertEquals(1, getNotificationCount(rule2));
        assertTrue(isRuleViolated(rule1));
        assertTrue(isRuleViolated(rule2));

        assertEquals(0, meterRegistry.get(Profiler.NON_VIOLATED_LOGS_METRIC_COUNTER_NAME).counter().count());
        assertEquals(2, meterRegistry.get(Profiler.VIOLATED_LOGS_METRIC_COUNTER_NAME).counter().count());
        assertEquals(1, meterRegistry.get(Profiler.MATCHED_LOGS_METRIC_COUNTER_NAME).counter().count());
    }

    @Test
    void shouldCreateAtMostOneNotificationPerHour() throws Exception {
        UUID ruleId = UUID.randomUUID();
        IdentityWrapper identity = new IdentityWrapper(IdentityType.PhoneNumber, "989127654876");
        String geometry = createRandomWkt();
        mockController.addRule(ruleId, "Test Rule", geometry, LocalDateTime.now().plusDays(1),
                identity, RuleType.Enter);
        insertRule(ruleId, "Test Rule", geometry, LocalDateTime.now().plusDays(1), identity, RuleType.Enter);
        evaluator.start();

        Log log1 = generateRandomLogAtGeometryPosition(geometry, GeometryPosition.INSIDE)
                .setTimestamp(getSampleTimestamp().getEpochSecond() * 1000)
                .setPhoneNumber(identity.identityValue())
                .build();
        Log log2 = generateRandomLogAtGeometryPosition(geometry, GeometryPosition.INSIDE)
                .setTimestamp((getSampleTimestamp().plusSeconds(100).getEpochSecond()) * 1000)
                .setPhoneNumber(identity.identityValue())
                .build();
        Log log3 = generateRandomLogAtGeometryPosition(geometry, GeometryPosition.INSIDE)
                .setTimestamp(getSampleTimestamp().plusSeconds(350).getEpochSecond() * 1000)
                .setPhoneNumber(identity.identityValue())
                .build();
        List<Log> logs = List.of(log1, log2, log3);
        for (Log log : logs) {
            produceLog(applicationConfig.sourceTopic(), log.toByteArray());
        }

        Awaitility.await()
                .pollInterval(Duration.ofMillis(5))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    int count = dsl.fetchCount(TrackedLog.TRACKED_LOG);
                    assertEquals(3, count);
                });

        for (Log log : logs) {
            assertTrue(isTrackedRecordPersisted(log, ruleId, true));
        }
        assertEquals(1, getNotificationCount(ruleId));
        assertTrue(isRuleViolated(ruleId));
    }

    private boolean isTrackedRecordPersisted(Log log, UUID ruleId, boolean isViolated) {
        return dsl.fetchExists(
                    dsl.selectFrom(TrackedLog.TRACKED_LOG)
                    .where(TrackedLog.TRACKED_LOG.LATITUDE.equal(log.getLocation().getLatitude()))
                    .and(TrackedLog.TRACKED_LOG.LONGITUDE.equal(log.getLocation().getLongitude()))
                    .and(TrackedLog.TRACKED_LOG.IS_VIOLATED.equal(isViolated))
                    .and(TrackedLog.TRACKED_LOG.RULE_ID.equal(ruleId))
                    .and(TrackedLog.TRACKED_LOG.CREATED_AT.isNotNull())
                );
    }

    private int getNotificationCount(UUID ruleId) {
        return dsl.fetchCount(
                dsl.selectFrom(Notification.NOTIFICATION)
                        .where(Notification.NOTIFICATION.SEEN.equal(false))
                        .and(Notification.NOTIFICATION.RULE_ID.equal(ruleId))
        );
    }

    private boolean isRuleViolated(UUID ruleId) {
        return dsl.fetchExists(
                dsl.selectFrom(Rule.RULE)
                        .where(Rule.RULE.ID.equal(ruleId))
                        .and(Rule.RULE.IS_VIOLATED.equal(true))
        );
    }

    private void insertRule(UUID ruleId, String title, String geometry, LocalDateTime expiresAt,
                            IdentityWrapper identity, RuleType ruleType) {
        dsl.insertInto(Rule.RULE)
                .columns(Rule.RULE.ID, Rule.RULE.TITLE, Rule.RULE.GEOMETRY_WKT, Rule.RULE.EXPIRES_AT,
                        Rule.RULE.IDENTITY_TYPE, Rule.RULE.IDENTITY_VALUE, Rule.RULE.RULE_TYPE)
                .values(ruleId, title, geometry, expiresAt, identity.identityType().name(),
                        identity.identityValue(), ruleType.name())
                .execute();
    }

    private Log.Builder generateRandomLogAtGeometryPosition(String geometry,
            GeometryPosition geometryPosition) throws Exception {
        LatLon randomLocation = null;
        if (geometryPosition == GeometryPosition.INSIDE) {
            randomLocation = getRandomPointInsideWkt(geometry);
        }
        if (geometryPosition == GeometryPosition.OUTSIDE) {
            randomLocation = getRandomPointOutsideWkt(geometry);
        }
        return CameraLogGenerator.randomLog()
                .generateLogBuilder()
                .setLocation(
                        Location
                                .newBuilder()
                                .setLatitude(randomLocation.latitude())
                                .setLongitude(randomLocation.longitude())
                                .build()
                );
    }

    private List<Log> getRandomNoisyLogs() {
        List<Log> logs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            logs.add(CameraLogGenerator.randomLog().generateLogBuilder().build());
        }
        return logs;
    }

    private Instant getSampleTimestamp() {
        LocalDateTime sampleDateTime = LocalDateTime.of(
                2026, 1, 11, 15, 0
        );

        return sampleDateTime
                .atZone(ZoneId.of("Asia/Tehran"))
                .toInstant();
    }

    private void produceLog(String topic, byte[] value) {
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, value);
        producer.send(producerRecord);
    }

    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties producerProps = new Properties();
        String bootstrapServers = KafkaExtension.getKafkaContainer().getBootstrapServers();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }

    private enum GeometryPosition {
        INSIDE,
        OUTSIDE
    }
}
