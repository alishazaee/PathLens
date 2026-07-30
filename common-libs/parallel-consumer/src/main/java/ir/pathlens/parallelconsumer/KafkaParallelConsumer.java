package ir.pathlens.parallelconsumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A parallel consumer that decouples polling from processing via internal queues, and tracks offsets for at-least-once
 * delivery semantics.
 */
public class KafkaParallelConsumer<K, V> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaParallelConsumer.class);
    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

    private final KafkaConsumer<K, V> consumer;
    private final BlockingDeque<ConsumerRecord<K, V>> polledRecords;
    private final BlockingDeque<ConsumerRecord<K, V>> ackedRecordsQueue;
    private final Duration pollTimeout;
    private final Tracker<K, V> tracker;
    private final String topic;
    private volatile boolean running;
    private Thread pollThread;

    KafkaParallelConsumer(Builder<K, V> builder) {
        pollTimeout = builder.pollTimeout;
        ackedRecordsQueue = new LinkedBlockingDeque<>();
        polledRecords = new LinkedBlockingDeque<>(builder.queueSize);
        consumer = new KafkaConsumer<>(builder.properties);
        running = true;
        tracker = new Tracker<>(consumer);
        this.topic = builder.topic;
    }

    /**
     * Starts the background polling thread and subscribes to the configured topic.
     */
    public void start() {
        consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logger.info("Partitions revoked {}", partitions);
                drainCommittedMessages();
                tracker.commitSync(partitions);
                tracker.removePartitions(partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.info("Assigned {}", partitions);
            }
        });

        pollThread = new Thread(this::pollLoop, "poll-thread");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    /**
     * Returns the next available record from the polled queue, or null if empty.
     */
    public ConsumerRecord<K, V> poll() {
        return polledRecords.poll();
    }

    /**
     * Marks a record as processed so its offset can be committed.
     */
    public void ack(ConsumerRecord<K, V> record) {
        ackedRecordsQueue.add(record);
    }


    @Override
    public void close() throws Exception {
        running = false;
        consumer.wakeup();
        if (pollThread != null) {
            pollThread.join();
        }
    }

    private void pollLoop() {
        try {
            while (running) {
                try {
                    ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                    for (ConsumerRecord<K, V> record : records) {
                        tracker.track(record);
                        drainCommittedMessages();
                        while (!polledRecords.offer(record)) {
                            drainCommittedMessages();
                            Thread.sleep(1);
                        }
                    }
                } catch (WakeupException e) {
                    logger.info("consuming records is shutting down during polling time...");
                }
            }
        } catch (InterruptedException e) {
            logger.info("consuming records is shutting down ...");
            Thread.currentThread().interrupt();
        } finally {
            drainCommittedMessages();
            consumer.close();
        }
    }

    private void drainCommittedMessages() {
        ConsumerRecord<K, V> record;
        while ((record = ackedRecordsQueue.poll()) != null) {
            tracker.complete(record);
        }
    }

    /**
     * Builder for {@link KafkaParallelConsumer}.
     */
    public static class Builder<K, V> {
        private Properties properties;
        private int queueSize;
        private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
        private String topic;

        public Builder<K, V> withProperties(Properties properties) {
            if (properties == null) {
                throw new IllegalArgumentException("properties must not be null");
            }
            this.properties = properties;
            this.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");;
            return this;
        }

        public Builder<K, V> withQueueSize(int queueSize) {
            if (queueSize <= 0) {
                throw new IllegalArgumentException("queueSize must be positive, got: " + queueSize);
            }
            this.queueSize = queueSize;
            return this;
        }

        public Builder<K, V> withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder<K, V> withPollTimeout(Duration pollTimeout) {
            if (pollTimeout == null || pollTimeout.isNegative()) {
                throw new IllegalArgumentException("pollTimeout must be non-negative");
            }
            this.pollTimeout = pollTimeout;
            return this;
        }

        public KafkaParallelConsumer<K, V> build() {
            Validate.notNull(properties, "properties must not be null");
            Validate.notNull(topic, "topic can not be null");
            Validate.isTrue(queueSize > 0, "queueSize must be positive");
            return new KafkaParallelConsumer<>(this);
        }
    }
}
