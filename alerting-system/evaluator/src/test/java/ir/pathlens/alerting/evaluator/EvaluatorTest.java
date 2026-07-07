package ir.pathlens.alerting.evaluator;

import static ir.pathlens.GeometryUtils.createRandomWkt;
import static ir.pathlens.GeometryUtils.getRandomPointInsideWkt;
import static ir.pathlens.GeometryUtils.getRandomPointOutsideWkt;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import ir.pathlens.GeometryUtils.LatLon;
import ir.pathlens.alerting.evaluator.configs.ApplicationConfig;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.RuleType;
import ir.pathlens.alerting.rest.controller.MockRuleController;
import ir.pathlens.extension.kafka.KafkaExtension;
import ir.pathlens.generator.CameraLogGenerator;
import ir.pathlens.proto.CameraLogProto.Location;
import ir.pathlens.proto.CameraLogProto.Log;
import ir.pathlens.proto.TargetLogProto;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@ExtendWith({KafkaExtension.class})
class EvaluatorTest {

    private MockRuleController mockController;
    private ApplicationConfig applicationConfig;
    private MeterRegistry meterRegistry;
    private Producer<byte[], byte[]> producer;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private Evaluator evaluator;

    @BeforeEach
    void setup() throws IOException {
        applicationConfig = ConfigReader.loadConfig(Path.of("src/test/resources/application.yml"));
        mockController = new MockRuleController();

        applicationConfig.rulesCacheConfig().setBaseUrl(mockController.getBaseUrl());
        String bootstrapServers = KafkaExtension.getKafkaContainer().getBootstrapServers();
        applicationConfig.kafkaProducerConfig().setBootstrapServers(bootstrapServers);
        applicationConfig.kafkaConsumerConfig().setBootstrapServers(bootstrapServers);
        meterRegistry =  new SimpleMeterRegistry();
        evaluator = new Evaluator(applicationConfig, meterRegistry);
        producer = createProducer();
        kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(List.of(applicationConfig.destinationTopic()));
    }

    @AfterEach
    void tearDown() throws Exception {
        evaluator.close();
        if (producer != null) {
            producer.close();
        }
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
        mockController.close();
    }

    @Test
    void testRuleMatchingDetection() throws Exception {
        UUID ruleId = UUID.randomUUID();
        IdentityWrapper identity = new IdentityWrapper(IdentityType.PlateNumber, "IR12AB345");
        String geometry = createRandomWkt();
        mockController.addRule(ruleId, "Test Rule", geometry,
                LocalDateTime.now().plusDays(1), identity, RuleType.Enter);
        evaluator.start();

        List<Log> logs = getRandomNoisyLogs();
        Log log1 = getRandomViolatedLog(geometry, RuleType.Enter).setPlateNumber(identity.identityValue()).build();
        Log log2 = getRandomViolatedLog(geometry, RuleType.Exit).setPlateNumber(identity.identityValue()).build();
        Log log3 = getRandomViolatedLog(geometry, RuleType.Enter).setPlateNumber(identity.identityValue()).build();
        Log log4 = getRandomViolatedLog(geometry, RuleType.Exit).setPlateNumber(identity.identityValue()).build();
        logs.addAll(List.of(log1, log2, log3, log4));
        Collections.shuffle(logs);

        for (Log log : logs) {
            produceLog(applicationConfig.sourceTopic(), log.toByteArray());
        }

        List<TargetLogProto.TargetLog> outputTargets = new ArrayList<>();
        Awaitility.await()
                .pollInterval(Duration.ofMillis(50))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        if (record.value() != null) {
                            TargetLogProto.TargetLog targetLog = TargetLogProto.TargetLog.parseFrom(record.value());
                            outputTargets.add(targetLog);
                        }
                    }
                    assertEquals(4, outputTargets.size());
                });

        assertEquals(2,
                outputTargets.stream()
                        .filter(target -> isViolatedForRule(target, ruleId))
                        .count());

        assertEquals(1, getActualMatchedLocationsCount(outputTargets, log1));
        assertEquals(1, getActualMatchedLocationsCount(outputTargets, log2));
        assertEquals(1, getActualMatchedLocationsCount(outputTargets, log3));
        assertEquals(1, getActualMatchedLocationsCount(outputTargets, log4));

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

        evaluator.start();
        List<Log> logs = getRandomNoisyLogs();
        Log violatedLog = getRandomViolatedLog(geometry, RuleType.Enter)
                .setPlateNumber(identity1.identityValue())
                .setPhoneNumber(identity2.identityValue())
                .build();
        logs.add(violatedLog);
        Collections.shuffle(logs);

        for (Log log : logs) {
            produceLog(applicationConfig.sourceTopic(), log.toByteArray());
        }
        List<TargetLogProto.TargetLog> outputTargets = new ArrayList<>();
        Awaitility.await()
                .pollInterval(Duration.ofMillis(50))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        if (record.value() != null) {
                            TargetLogProto.TargetLog targetLog = TargetLogProto.TargetLog.parseFrom(record.value());
                            outputTargets.add(targetLog);
                        }
                    }
                    assertEquals(2, outputTargets.size());
                });

        assertEquals(1,
                outputTargets.stream()
                        .filter(target -> isViolatedForRule(target, rule1))
                        .count());
        assertEquals(1,
                outputTargets.stream()
                        .filter(target -> isViolatedForRule(target, rule2))
                        .count());
        assertEquals(2, getActualMatchedLocationsCount(outputTargets, violatedLog));

        assertEquals(0, meterRegistry.get(Profiler.NON_VIOLATED_LOGS_METRIC_COUNTER_NAME).counter().count());
        assertEquals(2, meterRegistry.get(Profiler.VIOLATED_LOGS_METRIC_COUNTER_NAME).counter().count());
        assertEquals(1, meterRegistry.get(Profiler.MATCHED_LOGS_METRIC_COUNTER_NAME).counter().count());
    }

    private static boolean isViolatedForRule(TargetLogProto.TargetLog target, UUID ruleId) {
        return target.getViolated()
                && target.getRuleId().equals(ruleId.toString());
    }

    private static long getActualMatchedLocationsCount(List<TargetLogProto.TargetLog> targets, Log log) {
        return targets.stream().filter(target ->
                target.getLocation().getLatitude() == log.getLocation().getLatitude()
                        && target.getLocation().getLongitude() == log.getLocation().getLongitude()).count();
    }

    private Log.Builder getRandomViolatedLog(String geometry, RuleType ruleType) throws Exception {
        LatLon randomLocation = null;
        if (ruleType == RuleType.Enter) {
            randomLocation = getRandomPointInsideWkt(geometry);
        }
        if (ruleType == RuleType.Exit) {
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

    private KafkaConsumer<byte[], byte[]> createConsumer() {
        Properties consumerProps = new Properties();
        String bootstrapServers = KafkaExtension.getKafkaContainer().getBootstrapServers();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(consumerProps);
    }
}
