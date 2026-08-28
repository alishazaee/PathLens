package ir.pathlens.simulator;

import static ir.pathlens.simulator.Runner.createFakeDevices;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.device.client.DeviceClient;
import ir.pathlens.device.rest.controller.MockDeviceController;
import ir.pathlens.extension.kafka.KafkaExtension;
import ir.pathlens.proto.RawLogProto;
import ir.pathlens.simulator.configs.ApplicationConfig;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({KafkaExtension.class})
class TestSimulator {
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private MockDeviceController mockDeviceController;
    private DeviceClient deviceClient;
    private ApplicationConfig config;
    private List<String> deviceSerialNumbers;

    @BeforeEach
    void setup() throws Exception {
        mockDeviceController = new MockDeviceController();
        deviceClient = new DeviceClient(mockDeviceController.getBaseUrl());

        config = ConfigReader.loadConfig(Path.of("src/test/resources/application.yml"));
        config.setBootstrapServers(KafkaExtension.getKafkaContainer().getBootstrapServers());
        config.setDeviceApiUrl(mockDeviceController.getBaseUrl());
        deviceSerialNumbers = createFakeDevices(deviceClient, config.getNumberOfDevices());

        kafkaConsumer = createKafkaConsumer();
    }

    @AfterEach
    void tearDown() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
        if (deviceClient != null) {
            deviceClient.close();
        }
        if (mockDeviceController != null) {
            mockDeviceController.close();
        }
    }

    @Test
    void testSimulatorProducesLogsOnlyForRegisteredDeviceSerialNumbers() throws Exception {
        try (Simulator simulator = new Simulator(config, deviceSerialNumbers)) {
            simulator.start();
            List<RawLogProto.Log> collectedLogs = collectLogs(config.getMinBatchSize());

            assertTrue(collectedLogs.size() >= config.getMinBatchSize());

            Set<String> registeredSerialNumbers = new HashSet<>(deviceSerialNumbers);
            Set<String> observedSerialNumbers = new HashSet<>();
            for (RawLogProto.Log log : collectedLogs) {
                assertFalse(log.getDeviceSerialNumber().isBlank(),
                        "Log must be stamped with a device serial number");
                assertTrue(registeredSerialNumbers.contains(log.getDeviceSerialNumber()),
                        "Log must belong to a registered device, but was: " + log.getDeviceSerialNumber());
                assertFalse(log.getRecord().isBlank(), "Log must contain a camera record");
                assertFalse(log.getFilePath().isBlank(), "Log must contain a sensor file path");
                observedSerialNumbers.add(log.getDeviceSerialNumber());
            }

            assertEquals(new HashSet<>(deviceSerialNumbers), observedSerialNumbers,
                    "Simulator should distribute logs across all registered devices");
        }
    }

    @Test
    void testConfigValidationForInvalidBatchBounds() {
        ApplicationConfig invalidConfig = new ApplicationConfig();
        invalidConfig.setMinBatchSize(100);
        invalidConfig.setMaxBatchSize(100);
        assertThrows(IllegalArgumentException.class, invalidConfig::validate);

        invalidConfig.setMaxBatchSize(50);
        assertThrows(IllegalArgumentException.class, invalidConfig::validate);
    }

    private List<RawLogProto.Log> collectLogs(int minCount) throws Exception {
        List<RawLogProto.Log> collectedLogs = new ArrayList<>();
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> {
                    ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        collectedLogs.add(RawLogProto.Log.parseFrom(record.value()));
                    }
                    return collectedLogs.size() >= minCount;
                });
        return collectedLogs;
    }

    private KafkaConsumer<byte[], byte[]> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "simulator-test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()));
        return consumer;
    }
}
