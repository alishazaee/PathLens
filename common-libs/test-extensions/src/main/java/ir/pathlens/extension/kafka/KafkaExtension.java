package ir.pathlens.extension.kafka;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * JUnit5 extension that starts a Kafka container before all tests and cleans up topics after each test.
 */
public class KafkaExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback {
    private static final String TOPICS_KEY = "preExistingTopics";

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

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        Set<String> topics = listTopics();
        extensionContext.getStore(ExtensionContext.Namespace.create(getClass()))
                .put(TOPICS_KEY, topics);
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        Set<String> preExistingTopics = extensionContext.getStore(ExtensionContext.Namespace.create(getClass()))
                .get(TOPICS_KEY, Set.class);
        if (preExistingTopics != null) {
            deleteNewTopics(preExistingTopics);
        }
    }

    private Set<String> listTopics() throws Exception {
        try (AdminClient admin = createAdminClient()) {
            return admin.listTopics().names().get(10, TimeUnit.SECONDS).stream()
                    .filter(name -> !name.startsWith("__"))
                    .collect(Collectors.toSet());
        }
    }

    private void deleteNewTopics(Set<String> preExistingTopics) throws Exception {
        try (AdminClient admin = createAdminClient()) {
            Set<String> currentTopics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            Set<String> topicsToDelete = currentTopics.stream()
                    .filter(name -> !name.startsWith("__"))
                    .filter(name -> !preExistingTopics.contains(name))
                    .collect(Collectors.toSet());
            if (!topicsToDelete.isEmpty()) {
                admin.deleteTopics(topicsToDelete).all().get(10, TimeUnit.SECONDS);
            }
        }
    }

    private AdminClient createAdminClient() {
        return AdminClient.create(Collections.singletonMap(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()));
    }
}
