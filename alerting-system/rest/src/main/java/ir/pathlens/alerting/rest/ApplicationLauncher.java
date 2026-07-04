package ir.pathlens.alerting.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * This class defines a spring boot application entry point.
 */
@SpringBootApplication
public class ApplicationLauncher {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationLauncher.class);

    public static void main(String[] args) {
        logger.info("Starting alerting-rest application...");
        SpringApplication.run(ApplicationLauncher.class, args);
        logger.info("Alerting rest started successfully");
    }
}
