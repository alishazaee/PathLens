package ir.pathlens.processor;

import ir.pathlens.processor.configs.ApplicationConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Utility class for reading config file of application.
 */
public class ConfigReader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigReader.class);

    private ConfigReader() {
    }

    public static ApplicationConfig loadConfig(Path configYamlPath) {
        ApplicationConfig config;
        try (InputStream stream = Files.newInputStream(configYamlPath)) {
            config = new Yaml().loadAs(stream, ApplicationConfig.class);
        } catch (IOException e) {
            throw new AssertionError(String.format("The config yaml path %s doesn't exist or readable.",
                    configYamlPath));
        }
        return config;
    }
}
