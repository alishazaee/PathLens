package ir.pathlens.simulator;

import ir.pathlens.simulator.configs.ApplicationConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.yaml.snakeyaml.Yaml;

/**
 * Utility class for reading config file of application.
 */
public class ConfigReader {
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
        config.validate();
        return config;
    }
}
