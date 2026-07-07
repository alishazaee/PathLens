package ir.pathlens.alerting.evaluator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import ir.pathlens.alerting.evaluator.configs.ApplicationConfig;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Loads configs from a YAML file.
 */
public class ConfigReader {

    private static final ObjectMapper MAPPER =
            new ObjectMapper(new YAMLFactory());

    public static ApplicationConfig loadConfig(Path path) {
        try {
            return MAPPER.readValue(path.toFile(), ApplicationConfig.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config from " + path, e);
        }
    }
}