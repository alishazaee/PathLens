package ir.pathlens.parallelconsumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Tracks and commits offsets for records processed by the parallel consumer.
 */
public class Tracker<K, V> {
    private final KafkaConsumer<K, V> consumer;

    private final Map<TopicPartition, PartitionTracker> partitionTracker = new HashMap<>();

    public Tracker(KafkaConsumer<K, V> consumer) {
        this.consumer = consumer;
    }

    public void track(OffsetPartition offsetPartition) {
        TopicPartition topicPartition = offsetPartition.topicPartition();
        partitionTracker.computeIfAbsent(topicPartition, tp -> new PartitionTracker())
                .register(offsetPartition.offset());
    }

    public void complete(OffsetPartition offsetPartition) {
        TopicPartition topicPartition = offsetPartition.topicPartition();
        PartitionTracker tracker = partitionTracker.get(topicPartition);
        if (tracker == null) {
            return;
        }
        Optional<Long> commitOffset = tracker.complete(offsetPartition.offset());
        if (commitOffset.isPresent()) {
            consumer.commitAsync(Map.of(topicPartition, new OffsetAndMetadata(commitOffset.get() + 1)), null);
        }
    }

    /**
     * Synchronously commits the highest contiguous offset for the given partitions. Used during rebalance
     * (onPartitionsRevoked) and shutdown.
     */
    public void commitSync(Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : partitions) {
            PartitionTracker partitionTracker = this.partitionTracker.get(topicPartition);
            if (partitionTracker != null) {
                long nextOffset = partitionTracker.getNextCommitOffset();
                if (nextOffset != -1) {
                    offsets.put(topicPartition, new OffsetAndMetadata(nextOffset));
                }
            }
        }
        if (!offsets.isEmpty()) {
            consumer.commitSync(offsets);
        }
    }

    /**
     * Removes the tracker state for the given partitions. Called after partitions are revoked.
     */
    public void removePartitions(Collection<TopicPartition> partitions) {
        partitions.forEach(partitionTracker::remove);
    }
}
