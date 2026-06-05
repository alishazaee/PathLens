package ir.pathlens.alerting.rest.configs;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Application bean configuration.
 */
@Setter
@Getter
@Configuration
@ConfigurationProperties(prefix = "app")
public class ApplicationConfig {
    private KafkaConsumerConfig targetLogConsumer;
    private String targetLogsSourceTopic;
}
