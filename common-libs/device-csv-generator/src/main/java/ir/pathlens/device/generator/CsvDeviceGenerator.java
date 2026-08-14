package ir.pathlens.device.generator;

import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.DeviceType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates CSV records and files that describe devices, used in tests.
 */
public final class CsvDeviceGenerator {

    private static final String[] COUNTRIES = {"IRAN", "TURKEY", "UAE"};
    private static final String[] CITIES = {"TEHRAN", "ISTANBUL", "DUBAI"};
    private static final AtomicInteger SERIAL_COUNTER = new AtomicInteger(0);

    private CsvDeviceGenerator() {
    }

    /**
     * Generates a list of CSV lines, each line representing a device.
     *
     * <p>Line format: serialNumber,deviceType,status,site,country,city,latitude,longitude</p>
     */
    public static List<String> generateListOfCsvRecords(int count) {
        List<String> lines = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            lines.add(generateLine(i));
        }
        return lines;
    }

    /**
     * Generates a CSV file containing the given number of device records.
     */
    public static void generateFile(Path path, int count) throws IOException {
        List<String> lines = generateListOfCsvRecords(count);
        Files.write(path, lines, StandardCharsets.UTF_8);
    }

    private static String generateLine(int index) {
        DeviceType deviceType = DeviceType.values()[index % DeviceType.values().length];
        String country = COUNTRIES[index % COUNTRIES.length];
        String city = CITIES[index % CITIES.length];
        float latitude = round(35.0f + ThreadLocalRandom.current().nextFloat() * 5.0f);
        float longitude = round(51.0f + ThreadLocalRandom.current().nextFloat() * 5.0f);
        return String.join(",",
                "SN-" + SERIAL_COUNTER.incrementAndGet(),
                deviceType.name(),
                DeviceStatus.ACTIVE.name(),
                "SITE-" + index,
                country,
                city,
                Float.toString(latitude),
                Float.toString(longitude));
    }

    private static float round(float value) {
        return Math.round(value * 10000.0f) / 10000.0f;
    }
}
