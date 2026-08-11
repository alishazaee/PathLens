package ir.pathlens.alerting.evaluator;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import ir.pathlens.alerting.client.RulesCache;
import ir.pathlens.alerting.client.RulesClient;
import ir.pathlens.alerting.db.jooq.tables.records.TrackedLogRecord;
import ir.pathlens.alerting.evaluator.RuleViolationDetector.Result;
import ir.pathlens.alerting.evaluator.configs.ApplicationConfig;
import ir.pathlens.alerting.evaluator.configs.KafkaConsumerConfig;
import ir.pathlens.alerting.evaluator.configs.PostgresConfig;
import ir.pathlens.alerting.evaluator.configs.RulesCacheConfig;
import ir.pathlens.alerting.evaluator.persister.NotificationPersister;
import ir.pathlens.alerting.evaluator.persister.NotificationPersister.PersistRecord;
import ir.pathlens.alerting.evaluator.persister.PostgresWriter;
import ir.pathlens.client.ApiCallException;
import ir.pathlens.parallelconsumer.KafkaParallelConsumer;
import ir.pathlens.parallelconsumer.OffsetPartition;
import ir.pathlens.proto.CameraLogProto.Log;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

/**
 * Main entry point for evaluating camera logs against alerting rules. Consumes logs from Kafka, checks for rule
 * violations, and inserts notifications and tracked logs into postgres.
 */
public class Evaluator implements AutoCloseable {
    private final KafkaParallelConsumer<byte[], byte[]> parallelConsumer;
    private final ApplicationConfig applicationConfig;
    private final RulesCache rulesCache;
    private final ExecutorService executor;
    private final Profiler profiler;
    private final PostgresWriter postgresWriter;
    private final NotificationPersister notificationPersister;
    private final HikariDataSource dataSource;
    private volatile boolean running;

    public Evaluator(ApplicationConfig applicationConfig, MeterRegistry meterRegistry) {
        profiler = new Profiler(meterRegistry);
        this.parallelConsumer = createKafkaConsumer(applicationConfig.kafkaConsumerConfig());
        this.dataSource = createDataSource(applicationConfig.postgresConfig());
        DSLContext dsl = DSL.using(dataSource, SQLDialect.POSTGRES);

        postgresWriter = new PostgresWriter(dsl);
        notificationPersister = new NotificationPersister(applicationConfig, postgresWriter);
        RulesCacheConfig rulesCacheConfig = applicationConfig.rulesCacheConfig();
        RulesClient rulesClient = new RulesClient(rulesCacheConfig.getBaseUrl());
        executor = Executors.newFixedThreadPool(applicationConfig.threadCount());
        this.rulesCache = new RulesCache(rulesClient,
                rulesCacheConfig.getMinInitialDelayInMillis(),
                rulesCacheConfig.getMaxInitialDelayInMillis());
        this.applicationConfig = applicationConfig;
    }

    public void start() throws ApiCallException {
        running = true;
        notificationPersister.start();
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
        rulesCache.close();
        notificationPersister.close();
        dataSource.close();
    }

    @VisibleForTesting
    public static HikariDataSource createDataSource(PostgresConfig config) {
        HikariConfig hikariConfig = new HikariConfig();

        hikariConfig.setJdbcUrl(config.getUrl());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());

        hikariConfig.setMaximumPoolSize(config.getMaximumPoolSize());
        hikariConfig.setMinimumIdle(config.getMinimumIdle());

        hikariConfig.setConnectionTimeout(config.getConnectionTimeoutInMillis());
        hikariConfig.setIdleTimeout(config.getIdleTimeoutInMillis());
        hikariConfig.setMaxLifetime(config.getMaxLifetimeInMillis());

        return new HikariDataSource(hikariConfig);
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
        AtomicInteger remaining = new AtomicInteger(totalAlerts);

        for (UUID ruleId : result.violatedRules()) {
            TrackedLogRecord record = new TrackedLogRecord();
            record.setRuleId(ruleId);
            record.setLatitude(log.getLocation().getLatitude());
            record.setLongitude(log.getLocation().getLongitude());
            record.setIsViolated(true);
            record.setTimestamp(getTimestampInLocalDateTime(log.getTimestamp()));

            notificationPersister.persist(new PersistRecord(() -> {
                if (remaining.decrementAndGet() == 0) {
                    parallelConsumer.ack(offsetPartition);
                }
            }, record));
        }
        for (UUID ruleId : result.nonViolatedRules()) {
            TrackedLogRecord record = new TrackedLogRecord();
            record.setRuleId(ruleId);
            record.setLatitude(log.getLocation().getLatitude());
            record.setLongitude(log.getLocation().getLongitude());
            record.setIsViolated(false);
            record.setTimestamp(getTimestampInLocalDateTime(log.getTimestamp()));

            notificationPersister.persist(new PersistRecord(() -> {
                if (remaining.decrementAndGet() == 0) {
                    parallelConsumer.ack(offsetPartition);
                }
            }, record));
        }
        if (!result.violatedRules().isEmpty() || !result.nonViolatedRules().isEmpty()) {
            profiler.recordMatchedLog();
        }
        profiler.recordViolatedRules(result.violatedRules().size());
        profiler.recordNonViolatedRules(result.nonViolatedRules().size());
    }

    private LocalDateTime getTimestampInLocalDateTime(long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("Asia/Tehran"));
    }

    private Log parseLog(byte[] record) {
        try {
            return Log.parseFrom(record);
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("unexpected error happened, can not parse log", e);
        }
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
}
