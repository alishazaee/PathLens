package ir.pathlens.alerting.rest.service;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import ir.pathlens.alerting.rest.configs.ApplicationConfig;
import ir.pathlens.proto.TargetLogProto.TargetLog;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Listener for target log events.
 */
@Service
public class TargetLogEventListener {

    private final ParallelStreamProcessor<byte[], byte[]> targetLogsKafkaConsumer;
    private final ApplicationConfig applicationConfig;
    private final TargetLogBatchPersister targetLogBatchPersister;

    private static final Logger logger = LoggerFactory.getLogger(TargetLogEventListener.class);

    public TargetLogEventListener(
            ParallelStreamProcessor<byte[], byte[]> targetLogsKafkaConsumer,
            ApplicationConfig applicationConfig,
            TargetLogBatchPersister targetLogBatchPersister) {
        this.targetLogBatchPersister = targetLogBatchPersister;
        this.targetLogsKafkaConsumer = targetLogsKafkaConsumer;
        this.applicationConfig = applicationConfig;
    }

    @PostConstruct
    public void start() {
        logger.info("Starting target consuming...");

        targetLogsKafkaConsumer.subscribe(List.of(applicationConfig.getTargetLogsSourceTopic()));
        targetLogsKafkaConsumer.poll(batch -> {
            List<TargetLog> targets = batch.stream()
                    .map(record -> deserialize(record.value()))
                    .filter(java.util.Objects::nonNull)
                    .toList();
            targetLogBatchPersister.processBatch(targets);
        });
    }

    private TargetLog deserialize(byte[] payload) {
        try {
            return TargetLog.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to deserialize TargetLog protobuf message, skipping record", e);
            return null;
        }
    }

    @PreDestroy
    public void stop() {
        logger.info("Shutting down TargetLogEventListener...");
        targetLogsKafkaConsumer.close();
    }
}