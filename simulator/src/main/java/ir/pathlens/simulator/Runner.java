package ir.pathlens.simulator;

import static ir.pathlens.simulator.ConfigReader.loadConfig;

import com.google.common.annotations.VisibleForTesting;
import ir.pathlens.device.client.AlreadyExistsException;
import ir.pathlens.device.client.ApiCallException;
import ir.pathlens.device.client.DeviceClient;
import ir.pathlens.device.model.DeviceCreateRequestDto;
import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.DeviceType;
import ir.pathlens.device.model.LocationCreateDto;
import ir.pathlens.simulator.configs.ApplicationConfig;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the log simulator.
 */
public class Runner {

    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    private Runner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "One required argument (config path) should be provided.");
        }

        String configPath = args[0];

        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            logger.error("Crashed because of unhandled exception", e);
            Runtime.getRuntime().halt(-1);
        });

        logger.info("Starting log simulator application...");
        ApplicationConfig config = loadConfig(Paths.get(configPath));
        DeviceClient deviceClient = new DeviceClient(config.getDeviceApiUrl());;
        List<String> serialNumbers = createFakeDevices(deviceClient, config.getNumberOfDevices());
        Simulator simulator = new Simulator(config, serialNumbers);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("JVM is Shutting down....");
            simulator.close();
            deviceClient.close();

            logger.info("Bye!");
        }));

        simulator.start();
        logger.info("Simulator started successfully.");
    }

    @VisibleForTesting
    public static List<String> createFakeDevices(DeviceClient client, int numberOfDevices) throws ApiCallException {
        List<String> serialNumbers = new ArrayList<>();
        DeviceType[] types = DeviceType.values();
        LocationCreateDto[] locations = {
                new LocationCreateDto("SITE-TEHRAN", "IRAN", "TEHRAN", 35.6892f, 51.3890f),
                new LocationCreateDto("SITE-MASHHAD", "IRAN", "MASHHAD", 36.2921f, 59.6177f),
                new LocationCreateDto("SITE-ISFAHAN", "IRAN", "ISFAHAN", 32.6546f, 51.6680f),
                new LocationCreateDto("SITE-SHIRAZ", "IRAN", "SHIRAZ", 29.5918f, 52.5837f),
                new LocationCreateDto("SITE-TABRIZ", "IRAN", "TABRIZ", 38.0962f, 46.2738f)
        };

        for (int i = 0; i < numberOfDevices; i++) {
            LocationCreateDto location = locations[ThreadLocalRandom.current().nextInt(locations.length)];
            try {
                client.createNewLocation(location);
            } catch (AlreadyExistsException e) {
                logger.info("location %s already exist".formatted(location.site()));
            }

            String serialNumber = "DEVICE" + i;
            DeviceType type = types[ThreadLocalRandom.current().nextInt(types.length)];
            DeviceCreateRequestDto request = new DeviceCreateRequestDto(serialNumber, type, DeviceStatus.ACTIVE,
                    location.site());

            try {
                client.createNewDevice(request);
            } catch (AlreadyExistsException e) {
                logger.info("device %s already exist".formatted(serialNumber));
            }
            serialNumbers.add(serialNumber);
        }

        return serialNumbers;
    }
}
