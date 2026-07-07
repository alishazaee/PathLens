package ir.pathlens.parallelconsumer;

import org.apache.kafka.common.TopicPartition;

/**
 * Represents a (topic-partition, offset) pair for tracking committed offsets.
 */
public record OffsetPartition(TopicPartition topicPartition, long offset) {

}
