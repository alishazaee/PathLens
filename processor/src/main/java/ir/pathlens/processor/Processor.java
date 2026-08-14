package ir.pathlens.processor;

import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.MeterRegistry;
import ir.pathlens.device.cache.DeviceCache;
import ir.pathlens.device.client.ApiCallException;
import ir.pathlens.device.client.DeviceClient;
import ir.pathlens.parallelconsumer.KafkaParallelConsumer;
import ir.pathlens.parallelconsumer.OffsetPartition;
import ir.pathlens.processor.configs.ApplicationConfig;
import ir.pathlens.processor.configs.KafkaConsumerConfig;
import ir.pathlens.processor.configs.KafkaProducerConfig;
import ir.pathlens.proto.CameraLogProto.ErrorType;
import ir.pathlens.proto.RawLogProto;
import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes raw camera logs from Kafka, transforms and enriches them, and publishes the result to the destination or
 * trash topic.
 */
public class Processor implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Processor.class);
    private final Transformer transformer;
    private final Profiler profiler;
    private final KafkaParallelConsumer<byte[], byte[]> parallelConsumer;
    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    private final DeviceCache deviceCache;
    private final ApplicationConfig applicationConfig;
    private Thread pollThread;
    private final ExecutorService executor;
    private volatile boolean running;

    public Processor(MeterRegistry meterRegistry, ApplicationConfig applicationConfig) {
        DeviceClient client;
        try {
            client = new DeviceClient(applicationConfig.getDeviceCacheConfig().getBaseUrl());
        } catch (ApiCallException e) {
            throw new AssertionError("unexpected error happened", e);
        }
        profiler = new Profiler(meterRegistry);
        deviceCache = new DeviceCache(
                client,
                applicationConfig.getDeviceCacheConfig().getMinInitialDelayInMillis(),
                applicationConfig.getDeviceCacheConfig().getMaxInitialDelayInMillis(),
                applicationConfig.getDeviceCacheConfig().getSyncIntervalInMillis()
        );
        transformer = new Transformer(deviceCache);
        int queueSize = applicationConfig.getQueueSize();
        parallelConsumer = createKafkaConsumer(applicationConfig.getKafkaConsumerConfig(), queueSize);

        this.applicationConfig = applicationConfig;
        executor = Executors.newFixedThreadPool(applicationConfig.getThreadCount());
        kafkaProducer = createKafkaProducer(applicationConfig.getKafkaProducerConfig());
    }

    public void start() throws ApiCallException {
        running = true;
        deviceCache.sync();
        deviceCache.submitCacheSyncBackgroundTask();
        parallelConsumer.start(applicationConfig.getSourceTopic());
        pollThread = new Thread(this::pollLoop);
        pollThread.start();
    }

    private void pollLoop() {
        while (running) {
            Optional<ConsumerRecord<byte[], byte[]>> record = parallelConsumer.poll();
            if (record.isPresent()) {
                ConsumerRecord<byte[], byte[]> consumerRecord = record.get();
                executor.submit(() -> processRecord(consumerRecord.value(), consumerRecord.topic(),
                        consumerRecord.partition(), consumerRecord.offset()));
            }
        }
    }

    private void processRecord(byte[] value, String topic, int partition, long offset) {
        RawLogProto.Log rawLog = parseRawLog(value);
        TransformResult result = transformer.transform(rawLog);
        profiler.profile(result);
        String destinationTopic;
        if (result.getErrorType() == ErrorType.HARD) {
            destinationTopic = applicationConfig.getTrashTopic();
        } else {
            destinationTopic = applicationConfig.getDestinationTopic();
        }
        OffsetPartition offsetPartition = new OffsetPartition(new TopicPartition(topic, partition), offset);
        sendToKafka(result.getLog(), destinationTopic, offsetPartition);
    }

    private static KafkaParallelConsumer<byte[], byte[]> createKafkaConsumer(KafkaConsumerConfig kafkaConsumerConfig,
            int queueSize) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConsumerConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerConfig.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConsumerConfig.getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        if (kafkaConsumerConfig.getExtraConfigs() != null) {
            props.putAll(kafkaConsumerConfig.getExtraConfigs());
        }
        return new KafkaParallelConsumer.Builder<byte[], byte[]>()
                .withQueueSize(queueSize)
                .withProperties(props)
                .build();
    }

    private static KafkaProducer<byte[], byte[]> createKafkaProducer(KafkaProducerConfig kafkaProducerConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        if (kafkaProducerConfig.getExtraConfigs() != null) {
            props.putAll(kafkaProducerConfig.getExtraConfigs());
        }
        return new KafkaProducer<>(props);
    }

    private void sendToKafka(byte[] logBytes, String topic, OffsetPartition offsetPartition) {
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, logBytes);
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                parallelConsumer.ack(offsetPartition);
                return;
            }
            if (exception != null) {
                if (exception instanceof RetriableException) {
                    logger.warn("Retryable Kafka exception: " + exception);
                } else {
                    logger.error("Non-retryable Kafka exception", exception);
                    throw new AssertionError(exception);
                }
            }
        });
    }

    private RawLogProto.Log parseRawLog(byte[] rawRecord) {
        try {
            return RawLogProto.Log.parseFrom(rawRecord);
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("unexpected error happened, can not parse log", e);
        }
    }

    @Override
    public void close() {
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        kafkaProducer.flush();
        try {
            parallelConsumer.close();
        } catch (Exception e) {
            logger.error("Error while closing parallel consumer", e);
        }
        kafkaProducer.close(Duration.ofSeconds(5));
        deviceCache.close();
    }
}
