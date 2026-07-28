package ir.pathlens.parallelconsumer;

import static ir.pathlens.extension.kafka.KafkaExtension.getKafkaContainer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.extension.kafka.KafkaExtension;
import ir.pathlens.generator.CameraLogGenerator;
import ir.pathlens.proto.CameraLogProto;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@ExtendWith({KafkaExtension.class})
class KafkaParallelConsumerTest {
    private final KafkaProducer<byte[], byte[]> kafkaProducer = createKafkaProducer();

    @AfterEach
    void tearDown() {
        kafkaProducer.close();
    }

    @Test
    void testSuccessfulConsuming() throws Exception {
        // Verify that acknowledged offsets are committed and previously acknowledged records are not reprocessed.
        for (int i = 0; i < 10; i++) {
            try (KafkaParallelConsumer<byte[], byte[]> consumer = new KafkaParallelConsumer.Builder<byte[], byte[]>()
                            .withPollTimeout(Duration.ofMillis(5))
                            .withQueueSize(10)
                            .withTopic("test-1")
                            .withProperties(createKafkaConsumer("testy-1"))
                            .build()) {
                consumer.start();
                CameraLogProto.Log log1 = CameraLogGenerator.randomLog().generateLogBuilder().build();
                CameraLogProto.Log log2 = CameraLogGenerator.randomLog().generateLogBuilder().build();
                sendRecord(new ProducerRecord<>("test-1", log1.toByteArray()));
                sendRecord(new ProducerRecord<>("test-1", log2.toByteArray()));

                List<CameraLogProto.Log> logs = new ArrayList<>();
                Awaitility.await()
                        .atMost(Duration.ofSeconds(10))
                        .untilAsserted(() -> {
                            ConsumerRecord<byte[], byte[]> record = consumer.poll();
                            if (record != null) {
                                logs.add(CameraLogProto.Log.parseFrom(record.value()));
                                consumer.ack(record);
                            }
                            assertEquals(2, logs.size());
                        });
                assertTrue(logs.contains(log1));
                assertTrue(logs.contains(log2));
            }
        }
    }

    @Test
    void testConsumingWhenQueueSizeIsSmallerThanNumberOfMessages() throws Exception {
        try (KafkaParallelConsumer<byte[], byte[]> consumer = new KafkaParallelConsumer.Builder<byte[], byte[]>()
                .withPollTimeout(Duration.ofMillis(5))
                .withQueueSize(5)
                .withTopic("test-2")
                .withProperties(createKafkaConsumer("testy-2"))
                .build()) {
            for (int i = 0; i < 50; i++) {
                CameraLogProto.Log log = CameraLogGenerator.randomLog().generateLogBuilder().build();
                sendRecord(new ProducerRecord<>("test-2", log.toByteArray()));
            }
            consumer.start();
            List<CameraLogProto.Log> logs = new ArrayList<>();
            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        ConsumerRecord<byte[], byte[]> record = consumer.poll();
                        if (record != null) {
                            logs.add(CameraLogProto.Log.parseFrom(record.value()));
                            consumer.ack(record);
                        }
                        assertEquals(50, logs.size());
                    });
        }
    }

    private Properties createKafkaConsumer(String groupName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaContainer().getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

    private void sendRecord(ProducerRecord<byte[], byte[]> record) throws ExecutionException, InterruptedException {
        kafkaProducer.send(record).get();
    }

    private KafkaProducer<byte[], byte[]> createKafkaProducer() {
        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaContainer().getBootstrapServers());
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(producerProp);
    }
}
