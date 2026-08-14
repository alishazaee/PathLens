package ir.pathlens.processor;

import static ir.pathlens.processor.MetricName.CAMERA_LOGS_TOTAL;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import ir.pathlens.device.client.ApiCallException;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.LocationResponseDto;
import ir.pathlens.device.rest.controller.MockDeviceController;
import ir.pathlens.generator.RawLogGenerator;
import ir.pathlens.processor.configs.ApplicationConfig;
import ir.pathlens.proto.CameraLogProto;
import ir.pathlens.proto.CameraLogProto.ErrorType;
import ir.pathlens.proto.RawLogProto;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

class TestProcessor {

    private final List<String> sensorSerialNumbers = List.of("SN1", "SN2", "SN3", "SN4", "SN5");
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private KafkaContainer kafkaContainer;
    private static final MockDeviceController mockDeviceController = createMockDeviceController();
    private ApplicationConfig config;

    private static MockDeviceController createMockDeviceController() {
        try {
            return new MockDeviceController();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start mock device server", e);
        }
    }

    @BeforeEach
    void setup() {
        kafkaContainer = new KafkaContainer("apache/kafka-native:3.8.0");
        kafkaContainer.start();
        config = ConfigReader.loadConfig(Path.of("src/test/resources/application.yml"));
        config.getDeviceCacheConfig().setBaseUrl(mockDeviceController.getBaseUrl());
        config.getKafkaConsumerConfig().setBootstrapServers(kafkaContainer.getBootstrapServers());
        config.getKafkaProducerConfig().setBootstrapServers(kafkaContainer.getBootstrapServers());
        new DeviceRestProvisioner(mockDeviceController).populateMockServerWithDevices(sensorSerialNumbers);
        kafkaProducer = createKafkaProducer();
        kafkaConsumer = createKafkaConsumer();
    }

    @AfterEach
    void tearDown() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
    }

    @Test
    void testCorrectProcessing() throws ApiCallException {
        List<RawLogProto.Log> logs = new ArrayList<>();
        for (String serialNumber : sensorSerialNumbers) {
            RawLogProto.Log.Builder builder = RawLogGenerator.randomLog().setDeviceSerialNumber(serialNumber);
            logs.add(builder.build());
        }
        logs.stream().map(log -> log.toByteArray()).forEach(this::sendToKafka);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        try (Processor processor = new Processor(meterRegistry, config)) {
            processor.start();
            readOutputLogs(sensorSerialNumbers.size(), config.getDestinationTopic());
        }
        assertEquals(logs.size(), getTotalLogs(meterRegistry));
        assertEquals(sensorSerialNumbers.size(), getParsableLogs(meterRegistry));
        assertEquals(0, getNonParsableLogs(meterRegistry));
        assertEquals(0, getLocationNotEnrichedLogs(meterRegistry));
        assertEquals(sensorSerialNumbers.size(), getLocationEnrichedLogs(meterRegistry));
    }

    @Test
    void testHardErrorsTrashedLogs() throws ApiCallException {
        List<RawLogProto.Log> logs = new ArrayList<>();
        //insert valid and invalid logs
        for (String serialNumber : sensorSerialNumbers) {
            RawLogProto.Log.Builder validBuilder = RawLogGenerator.randomLog().setDeviceSerialNumber(serialNumber);
            RawLogProto.Log.Builder invalidSerialNumberBuilder = RawLogGenerator.randomLog().setDeviceSerialNumber("");

            logs.add(validBuilder.build());
            logs.add(invalidSerialNumberBuilder.build());
        }

        logs.stream().map(log -> log.toByteArray()).forEach(this::sendToKafka);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        try (Processor processor = new Processor(meterRegistry, config)) {
            processor.start();
            readOutputLogs(sensorSerialNumbers.size(), config.getDestinationTopic());
            readOutputLogs(sensorSerialNumbers.size(), config.getTrashTopic());
        }
        assertEquals(logs.size(), getTotalLogs(meterRegistry));
        assertEquals(sensorSerialNumbers.size() * 2, getParsableLogs(meterRegistry));
        assertEquals(0, getNonParsableLogs(meterRegistry));
        assertEquals(sensorSerialNumbers.size(), getLocationNotEnrichedLogs(meterRegistry));
        assertEquals(sensorSerialNumbers.size(), getLocationEnrichedLogs(meterRegistry));
    }

    @Test
    void testSoftErrorLog() throws ApiCallException {
        final String logSerialNumber = "SAMPLE_SN";
        DeviceResponseDto device = new DeviceResponseDto(
                325,
                logSerialNumber,
                null,
                DeviceStatus.ACTIVE,
                new LocationResponseDto(
                        "35723",
                        null,
                        "GHAZVIN",
                        12F,
                        3.2F
                )
        );
        mockDeviceController.addNewMockDevice(device);

        int numberOfLogs = 100;
        List<RawLogProto.Log> logs = new ArrayList<>();
        for (int i = 0; i < numberOfLogs; i++) {
            RawLogProto.Log.Builder log = RawLogGenerator.randomLog().setDeviceSerialNumber(logSerialNumber);
            logs.add(log.build());
        }
        logs.stream().map(log -> log.toByteArray()).forEach(this::sendToKafka);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        try (Processor processor = new Processor(meterRegistry, config)) {
            processor.start();
            readOutputLogs(numberOfLogs, config.getDestinationTopic());
        }
        assertEquals(numberOfLogs, getTotalLogs(meterRegistry));
        assertEquals(numberOfLogs, getParsableLogs(meterRegistry));
        assertEquals(0, getNonParsableLogs(meterRegistry));
        assertEquals(0, getLocationNotEnrichedLogs(meterRegistry));
        assertEquals(numberOfLogs, getLocationEnrichedLogs(meterRegistry));
        assertEquals(numberOfLogs, getSoftErrorsLogs(meterRegistry));
    }

    private double getTotalLogs(MeterRegistry meterRegistry) {
        return getCounterValue(meterRegistry, CAMERA_LOGS_TOTAL);
    }

    private double getParsableLogs(MeterRegistry meterRegistry) {
        return getCounterValue(meterRegistry, CAMERA_LOGS_TOTAL, "parsable", "true");
    }

    private double getNonParsableLogs(MeterRegistry meterRegistry) {
        return getCounterValue(meterRegistry, CAMERA_LOGS_TOTAL, "parsable", "false");
    }

    private double getLocationEnrichedLogs(MeterRegistry meterRegistry) {
        return getCounterValue(meterRegistry, CAMERA_LOGS_TOTAL, "location_enriched", "true");
    }

    private double getLocationNotEnrichedLogs(MeterRegistry meterRegistry) {
        return getCounterValue(meterRegistry, CAMERA_LOGS_TOTAL, "location_enriched", "false");
    }

    private double getSoftErrorsLogs(MeterRegistry meterRegistry) {
        return getCounterValue(meterRegistry, CAMERA_LOGS_TOTAL, "error_type", String.valueOf(ErrorType.SOFT));
    }

    private double getCounterValue(MeterRegistry meterRegistry, String name, String... tags) {
        return meterRegistry.find(name)
                .tags(tags)
                .meters()
                .stream()
                .filter(m -> m instanceof Counter)
                .mapToDouble(m -> ((Counter) m).count())
                .sum();
    }

    private void readOutputLogs(int numLogsToRead, String topicName) {
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        List<CameraLogProto.Log> collectedRecords = new ArrayList<>();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> {
                    ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(10));
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        collectedRecords.add(CameraLogProto.Log.parseFrom(record.value()));
                    }
                    return collectedRecords.size() == numLogsToRead;
                });
    }

    private KafkaProducer<byte[], byte[]> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<byte[], byte[]> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "processor-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private void sendToKafka(byte[] message) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(config.getSourceTopic(), message);
        kafkaProducer.send(record);
    }
}
