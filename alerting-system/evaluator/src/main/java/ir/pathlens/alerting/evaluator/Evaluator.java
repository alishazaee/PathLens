package ir.pathlens.alerting.evaluator;

import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.MeterRegistry;
import ir.pathlens.alerting.client.RulesCache;
import ir.pathlens.alerting.client.RulesClient;
import ir.pathlens.alerting.evaluator.RuleViolationDetector.Result;
import ir.pathlens.alerting.evaluator.configs.ApplicationConfig;
import ir.pathlens.alerting.evaluator.configs.KafkaConsumerConfig;
import ir.pathlens.alerting.evaluator.configs.KafkaProducerConfig;
import ir.pathlens.alerting.evaluator.configs.RulesCacheConfig;
import ir.pathlens.client.ApiCallException;
import ir.pathlens.parallelconsumer.KafkaParallelConsumer;
import ir.pathlens.parallelconsumer.OffsetPartition;
import ir.pathlens.proto.CameraLogProto.Log;
import ir.pathlens.proto.TargetLogProto.Location;
import ir.pathlens.proto.TargetLogProto.TargetLog;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
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
 * Main entry point for evaluating camera logs against alerting rules. Consumes logs from Kafka, checks for rule
 * violations, and produces. target logs to the destination topic.
 */
public class Evaluator implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Evaluator.class);

    private final KafkaParallelConsumer<byte[], byte[]> parallelConsumer;
    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    private final ApplicationConfig applicationConfig;
    private final RulesCache rulesCache;
    private final ExecutorService executor;
    private final Profiler profiler;
    private volatile boolean running;

    public Evaluator(ApplicationConfig applicationConfig, MeterRegistry meterRegistry) {
        profiler = new Profiler(meterRegistry);
        this.parallelConsumer = createKafkaConsumer(applicationConfig.kafkaConsumerConfig());
        RulesCacheConfig rulesCacheConfig = applicationConfig.rulesCacheConfig();
        RulesClient rulesClient = new RulesClient(rulesCacheConfig.getBaseUrl());
        executor = Executors.newFixedThreadPool(applicationConfig.threadCount());
        this.rulesCache = new RulesCache(rulesClient,
                rulesCacheConfig.getMinInitialDelayInMillis(),
                rulesCacheConfig.getMaxInitialDelayInMillis());
        this.applicationConfig = applicationConfig;
        this.kafkaProducer = createKafkaProducer(applicationConfig.kafkaProducerConfig());
    }

    public void start() throws ApiCallException {
        running = true;
        rulesCache.submitBackgroundTask();
        rulesCache.sync();
        parallelConsumer.start(applicationConfig.sourceTopic());
        executor.submit(this::pollLoop);
    }

    @Override
    public void close() throws Exception {
        running = false;
        executor.shutdown();

        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        parallelConsumer.close();
        kafkaProducer.close();
        rulesCache.close();
    }

    private void pollLoop() {
        while (running) {
            Optional<ConsumerRecord<byte[], byte[]>> record =  parallelConsumer.poll();
            if (record.isPresent()) {
                Log log = parseLog(record.get().value());
                TopicPartition topicPartition = new TopicPartition(record.get().topic(), record.get().partition());
                OffsetPartition offsetPartition = new OffsetPartition(topicPartition, record.get().offset());
                processRecord(log, offsetPartition);
            }
        }
    }

    private void processRecord(Log log, OffsetPartition offsetPartition) {
        RuleViolationDetector ruleViolationDetector = new RuleViolationDetector(rulesCache);
        Result result = ruleViolationDetector.findViolatedRules(log);
        int totalAlerts = result.nonViolatedRules().size() + result.violatedRules().size();
        if (totalAlerts == 0) {
            parallelConsumer.ack(offsetPartition);
            return;
        }
        AtomicInteger remainingSends = new AtomicInteger();
        for (UUID ruleId : result.violatedRules()) {
            TargetLog targetLog = TargetLog.newBuilder()
                    .setRuleId(ruleId.toString())
                    .setTimestamp(log.getTimestamp())
                    .setViolated(true)
                    .setLocation(Location.newBuilder()
                            .setLatitude(log.getLocation().getLatitude())
                            .setLongitude(log.getLocation().getLongitude())
                            .build())
                    .build();
            sendAlert(targetLog, offsetPartition, remainingSends);
        }
        for (UUID ruleId : result.nonViolatedRules()) {
            TargetLog targetLog = TargetLog.newBuilder()
                    .setRuleId(ruleId.toString())
                    .setTimestamp(log.getTimestamp())
                    .setViolated(false)
                    .setLocation(Location.newBuilder()
                            .setLatitude(log.getLocation().getLatitude())
                            .setLongitude(log.getLocation().getLongitude())
                            .build())
                    .build();
            sendAlert(targetLog, offsetPartition, remainingSends);
        }
        if (!result.violatedRules().isEmpty() || !result.nonViolatedRules().isEmpty()) {
            profiler.recordMatchedLog();
        }
        profiler.recordViolatedRules(result.violatedRules().size());
        profiler.recordNonViolatedRules(result.nonViolatedRules().size());
    }

    private Log parseLog(byte[] record) {
        try {
            return Log.parseFrom(record);
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("unexpected error happened, can not parse log", e);
        }
    }

    private void sendAlert(TargetLog targetLog, OffsetPartition offsetPartition, AtomicInteger remainingSends) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                applicationConfig.destinationTopic(), targetLog.toByteArray());
        kafkaProducer.send(record,
                getCallBack(applicationConfig.destinationTopic(), targetLog, offsetPartition, remainingSends));
    }

    private Callback getCallBack(String targetTopic, TargetLog targetLog, OffsetPartition offsetPartition,
                                 AtomicInteger remainingSends) {
        return (metadata, exception) -> {
            if (exception == null) {
                if (remainingSends.decrementAndGet() == 0) {
                    parallelConsumer.ack(offsetPartition);
                }
                return;
            }
            if (!(exception instanceof RetriableException)) {
                logger.error(
                        "Crash because of an error on sending record to kafka targetTopicName: " + "{} .", targetTopic,
                        exception);
                // Note that unlike the exit() method, halt() method does not cause shutdown hooks to be started.
                Runtime.getRuntime().halt(-1);
            }
        };
    }

    private static KafkaParallelConsumer<byte[], byte[]> createKafkaConsumer(KafkaConsumerConfig kafkaConsumerConfig) {
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
                .withPollTimeout(Duration.ofMillis(5))
                .withQueueSize(kafkaConsumerConfig.getQueueSize())
                .withProperties(props)
                .build();
    }

    private static KafkaProducer<byte[], byte[]> createKafkaProducer(
            KafkaProducerConfig kafkaProducerConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        if (kafkaProducerConfig.getExtraConfigs() != null) {
            props.putAll(kafkaProducerConfig.getExtraConfigs());
        }
        return new KafkaProducer<>(props);
    }
}
