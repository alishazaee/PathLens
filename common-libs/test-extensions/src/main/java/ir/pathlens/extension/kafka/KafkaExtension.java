package ir.pathlens.extension.kafka;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * JUnit5 extension that starts a Kafka container before all tests.
 */
public class KafkaExtension implements BeforeAllCallback {
    @Container
    static final KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    public static KafkaContainer getKafkaContainer() {
        return kafkaContainer;
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        if (!kafkaContainer.isRunning()) {
            kafkaContainer.start();
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaContainer::stop));
        }
    }
}
