package ir.pathlens.alerting.rest.configs;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for target log messages.
 */
@Component
public class TargetLogKafkaConsumer {

    @Bean
    public ParallelStreamProcessor<byte[], byte[]> targetLogsKafkaConsumer(ApplicationConfig config) {
        KafkaConsumerConfig kafkaConsumerConfig = config.getTargetLogConsumer();
        KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer(kafkaConsumerConfig);

        ParallelConsumerOptions<byte[], byte[]> options =
                ParallelConsumerOptions.<byte[], byte[]>builder()
                        .consumer(consumer)
                        .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                        .maxConcurrency(kafkaConsumerConfig.maxConcurrency())
                        .batchSize(kafkaConsumerConfig.batchSize())
                        .commitInterval(Duration.ofMillis(kafkaConsumerConfig.commitIntervalMs()))
                        .build();

        return ParallelStreamProcessor.createEosStreamProcessor(options);
    }

    private KafkaConsumer<byte[], byte[]> createKafkaConsumer(KafkaConsumerConfig config) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetReset());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        if (config.extraConfigs() != null) {
            props.putAll(config.extraConfigs());
        }
        return new KafkaConsumer<>(props);
    }
}
