package ir.pathlens.device.rest;

import static ir.pathlens.extension.postgresql.SpringCommonPostgresConfigs.registerPostgresProperties;

import ir.pathlens.device.rest.repository.DeviceRepository;
import ir.pathlens.device.rest.repository.LocationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Base class for controller integration tests: boots the Spring application on a random port and wires the shared
 * PostgreSQL test container (started by {@code PostgresqlExtension}) into the Spring environment.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class BaseControllerTest {

    @Autowired
    protected DeviceRepository deviceRepository;

    @Autowired
    protected LocationRepository locationRepository;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registerPostgresProperties(registry);
    }

    protected void cleanDatabase() {
        deviceRepository.deleteAll();
        locationRepository.deleteAll();
    }
}
