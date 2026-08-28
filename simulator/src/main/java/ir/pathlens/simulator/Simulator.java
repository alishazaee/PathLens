package ir.pathlens.simulator;

import ir.pathlens.camera.Constants.IpVersion;
import ir.pathlens.device.client.ApiCallException;
import ir.pathlens.generator.RawLogGenerator;
import ir.pathlens.proto.RawLogProto;
import ir.pathlens.simulator.configs.ApplicationConfig;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates random raw logs and produces them to Kafka as raw processor input.
 */
public class Simulator implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Simulator.class);

    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    private final ApplicationConfig applicationConfig;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong totalLogsProduced = new AtomicLong(0);
    private final List<String> deviceSerialNumbers;
    private final AtomicInteger deviceIndex = new AtomicInteger(0);
    private volatile boolean running;

    public Simulator(ApplicationConfig applicationConfig, List<String> deviceSerialNumbers) throws ApiCallException {
        this.applicationConfig = applicationConfig;
        this.deviceSerialNumbers = deviceSerialNumbers;
        this.kafkaProducer = createKafkaProducer(applicationConfig);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "simulator-producer");
            thread.setDaemon(false);
            return thread;
        });
    }

    public void start() {
        running = true;
        long intervalMillis = applicationConfig.getIntervalMillis();
        logger.info("Starting simulator...");
        scheduler.scheduleAtFixedRate(() -> produceBatch(), 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void produceBatch() {
        int batchSize = ThreadLocalRandom.current().nextInt(
                applicationConfig.getMinBatchSize(), applicationConfig.getMaxBatchSize());
        if (!running) {
            return;
        }

        long startTime = System.currentTimeMillis();
        IpVersion ipVersion = IpVersion.values()[ThreadLocalRandom.current().nextInt(IpVersion.values().length)];
        List<RawLogProto.Log> logs = RawLogGenerator.randomLogs(batchSize, ipVersion);
        for (RawLogProto.Log rawLog : logs) {
            RawLogProto.Log logToSend;
            int idx = deviceIndex.getAndIncrement() % deviceSerialNumbers.size();
            logToSend = rawLog.toBuilder().setDeviceSerialNumber(deviceSerialNumbers.get(idx)).build();

            byte[] payload = logToSend.toByteArray();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(applicationConfig.getTopic(), payload);

            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to produce log for device: {}",
                            logToSend.getDeviceSerialNumber(), exception);
                }
            });
        }

        long batchDuration = System.currentTimeMillis() - startTime;
        long total = totalLogsProduced.addAndGet(batchSize);

        logger.info("Batch sent: {} logs, total: {}, duration: {} ms", batchSize, total, batchDuration);

        if (batchDuration > applicationConfig.getIntervalMillis()) {
            logger.warn("Batch production ({}) exceeded interval ({}). Consider reducing batchSize.",
                    batchDuration, applicationConfig.getIntervalMillis());
        }
    }

    private static KafkaProducer<byte[], byte[]> createKafkaProducer(ApplicationConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        logger.info("Shutting down simulator...");
        running = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        kafkaProducer.flush();
        kafkaProducer.close(Duration.ofSeconds(5));
        logger.info("Simulator stopped. Total logs produced: {}", totalLogsProduced.get());
    }
}
