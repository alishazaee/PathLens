package ir.pathlens.processor;

import static ir.pathlens.proto.CameraLogProto.Error.DEVICE_NOT_FOUND;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_CITY;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_COUNTRY;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_DEVICE_TYPE;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_LATITUDE;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_LONGITUDE;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_SITE_ID;
import static ir.pathlens.proto.CameraLogProto.ErrorType.HARD;
import static ir.pathlens.proto.CameraLogProto.ErrorType.SOFT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.device.cache.DeviceCache;
import ir.pathlens.device.client.ApiCallException;
import ir.pathlens.device.client.DeviceClient;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.DeviceType;
import ir.pathlens.device.model.LocationResponseDto;
import ir.pathlens.device.rest.controller.MockDeviceController;
import ir.pathlens.generator.CameraLogGenerator;
import ir.pathlens.processor.configs.ApplicationConfig;
import ir.pathlens.processor.configs.DeviceCacheConfig;
import ir.pathlens.proto.CameraLogProto;
import ir.pathlens.proto.CameraLogProto.Error;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestEnricher {

    private static final String WITHOUT_COUNTRY_AND_CITY_FIELDS_SERIAL_NUMBER = "without_country_and_city_sn";
    private static final String WITHOUT_TYPE_FIELDS_SERIAL_NUMBER =
            "without_device_status_and_type_sn";
    private static final String WITHOUT_LATITUDE_FIELD_SERIAL_NUMBER = "without_latitude_sn";
    private static final String WITHOUT_LONGITUDE_FIELD_SERIAL_NUMBER = "without_longitude_sn";
    private static final String WITHOUT_SITE_ID_FIELD_SERIAL_NUMBER = "without_site_id_sn";

    protected DeviceCache deviceCache;
    protected static final MockDeviceController mockDeviceController = createMockDeviceController();
    protected ApplicationConfig config;

    private static MockDeviceController createMockDeviceController() {
        try {
            return new MockDeviceController();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start mock device server", e);
        }
    }

    @BeforeEach
    void setup() throws ApiCallException {
        config = ConfigReader.loadConfig(Path.of("src/test/resources/application.yml"));
        config.getDeviceCacheConfig().setBaseUrl(mockDeviceController.getBaseUrl());
        DeviceCacheConfig cacheConfig = config.getDeviceCacheConfig();

        DeviceClient client = new DeviceClient(cacheConfig.getBaseUrl());
        deviceCache = new DeviceCache(client, cacheConfig.getMinInitialDelayInMillis(),
                cacheConfig.getMaxInitialDelayInMillis(), cacheConfig.getSyncIntervalInMillis());
        populateMockServerWithDevices();
        deviceCache.sync();
    }

    @Test
    void testLocationSoftError() {
        CameraLogProto.Log.Builder log = CameraLogGenerator.randomLog().generateLogBuilder();
        Enricher enricher = new Enricher(deviceCache);
        enricher.enrich(log, WITHOUT_COUNTRY_AND_CITY_FIELDS_SERIAL_NUMBER);
        assertEquals(SOFT, log.getErrorType());
        assertTrue(log.getErrorSummaryList().containsAll(List.of(INVALID_CITY, INVALID_COUNTRY)));
        assertEquals(2, log.getErrorSummaryList().size());
        assertNotNull(log.getRawRecord());
    }

    @Test
    void testLocationHardError() {
        for (Entry<String, Error> serialNumberToErrorMap : Map.of(WITHOUT_LATITUDE_FIELD_SERIAL_NUMBER,
                INVALID_LATITUDE, WITHOUT_LONGITUDE_FIELD_SERIAL_NUMBER, INVALID_LONGITUDE,
                WITHOUT_SITE_ID_FIELD_SERIAL_NUMBER, INVALID_SITE_ID).entrySet()) {
            CameraLogProto.Log.Builder log = CameraLogGenerator.randomLog().generateLogBuilder();
            Enricher enricher = new Enricher(deviceCache);
            enricher.enrich(log, serialNumberToErrorMap.getKey());
            assertEquals(HARD, log.getErrorType());
            assertTrue(log.getErrorSummaryList().contains(serialNumberToErrorMap.getValue()));
            assertEquals(1, log.getErrorSummaryList().size());
            assertNotNull(log.getRawRecord());
        }
    }

    @Test
    void testDeviceSoftError() {
        CameraLogProto.Log.Builder log = CameraLogGenerator.randomLog().generateLogBuilder();
        Enricher enricher = new Enricher(deviceCache);
        enricher.enrich(log, WITHOUT_TYPE_FIELDS_SERIAL_NUMBER);
        assertEquals(SOFT, log.getErrorType());
        assertTrue(log.getErrorSummaryList().containsAll(List.of(INVALID_DEVICE_TYPE)));
        assertEquals(1, log.getErrorSummaryList().size());
        assertNotNull(log.getRawRecord());
    }


    @Test
    void testDeviceHardError() {
        CameraLogProto.Log.Builder log = CameraLogGenerator.randomLog().generateLogBuilder();
        Enricher enricher = new Enricher(deviceCache);
        enricher.enrich(log, "INVALID");
        assertEquals(HARD, log.getErrorType());
        assertTrue(log.getErrorSummaryList().contains(DEVICE_NOT_FOUND));
        assertEquals(1, log.getErrorSummaryList().size());
        assertNotNull(log.getRawRecord());
    }

    private void populateMockServerWithDevices() {
        DeviceResponseDto withOutLocationFields = new DeviceResponseDto(
                1,
                WITHOUT_COUNTRY_AND_CITY_FIELDS_SERIAL_NUMBER,
                DeviceType.SPEED_CAMERA,
                DeviceStatus.ACTIVE,
                new LocationResponseDto(
                        "SITE-1",
                        null,
                        null,
                        12.2F,
                        12.2F
                )
        );
        mockDeviceController.addNewMockDevice(withOutLocationFields);

        DeviceResponseDto withoutLatitude = new DeviceResponseDto(
                2,
                WITHOUT_LATITUDE_FIELD_SERIAL_NUMBER,
                DeviceType.TOLL_CAMERA,
                DeviceStatus.ACTIVE,
                new LocationResponseDto(
                        "SITE-2",
                        "IRAN",
                        "GHAZVIN",
                        null,
                        3.2F
                )
        );
        mockDeviceController.addNewMockDevice(withoutLatitude);

        DeviceResponseDto withoutLongitude = new DeviceResponseDto(
                3,
                WITHOUT_LONGITUDE_FIELD_SERIAL_NUMBER,
                DeviceType.RED_LIGHT_CAMERA,
                DeviceStatus.ACTIVE,
                new LocationResponseDto(
                        "SITE-3",
                        "IRAN",
                        "GORGAN",
                        44.44F,
                        null
                )
        );
        mockDeviceController.addNewMockDevice(withoutLongitude);

        DeviceResponseDto withoutDeviceFields = new DeviceResponseDto(
                4,
                WITHOUT_TYPE_FIELDS_SERIAL_NUMBER,
                null,
                DeviceStatus.ACTIVE,
                new LocationResponseDto(
                        "SITE-4",
                        "IRAN",
                        "ZANJAN",
                        12.2F,
                        13F
                )
        );
        mockDeviceController.addNewMockDevice(withoutDeviceFields);

        DeviceResponseDto withoutSiteId = new DeviceResponseDto(
                5,
                WITHOUT_SITE_ID_FIELD_SERIAL_NUMBER,
                DeviceType.RED_LIGHT_CAMERA,
                DeviceStatus.ACTIVE,
                new LocationResponseDto(
                        null,
                        "IRAN",
                        "ZANJAN",
                        12.2F,
                        13F
                )
        );
        mockDeviceController.addNewMockDevice(withoutSiteId);
    }
}
