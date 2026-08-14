package ir.pathlens.device.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.device.client.ApiCallException;
import ir.pathlens.device.client.DeviceClient;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.DeviceType;
import ir.pathlens.device.model.LocationResponseDto;
import ir.pathlens.device.rest.controller.MockDeviceController;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeviceCacheTest {

    private DeviceClient client;
    private MockDeviceController mockDeviceController;
    private List<DeviceResponseDto> deviceRecords;

    @BeforeEach
    void setup() throws IOException, ApiCallException {
        mockDeviceController = new MockDeviceController();
        client = new DeviceClient(mockDeviceController.getBaseUrl());
        deviceRecords = new ArrayList<>();

        populateMockServerWithDevices(100);
    }

    @AfterEach
    void tearDown() {
        mockDeviceController.close();
    }

    @Test
    void testCorrectCacheSynchronization() throws ApiCallException {
        try (DeviceCache deviceCache = new DeviceCache(client, 10, 1000, 1000)) {
            deviceCache.sync();
            awaitSync(deviceCache);

            deviceRecords.forEach(dto ->
                    assertEquals(dto, deviceCache.findByDeviceSerialNumber(dto.serialNumber()).get()));
        }
    }

    @Test
    void testRevisionUpdate() throws ApiCallException {
        try (DeviceCache deviceCache = new DeviceCache(client, 10, 100, 100)) {

            deviceCache.submitCacheSyncBackgroundTask();
            deviceCache.sync();
            awaitSync(deviceCache);

            Long initialRevision = deviceCache.getRevisionNumber();
            assertNotEquals(0, initialRevision);

            populateMockServerWithDevices(50);

            Awaitility.await()
                    .atMost(Duration.ofMillis(2000))
                    .pollDelay(Duration.ofMillis(200))
                    .until(() -> deviceCache.getTotalDevices() == deviceRecords.size());

            assertTrue(initialRevision < deviceCache.getRevisionNumber());
            assertEquals(150, deviceCache.getTotalDevices());
        }
    }

    private void populateMockServerWithDevices(int count) {
        AtomicInteger id = new AtomicInteger(deviceRecords.size() + 1);

        for (int i = 0; i < count; i++) {
            int deviceId = id.getAndIncrement();
            DeviceResponseDto dto = new DeviceResponseDto(
                    deviceId,
                    "SN-" + deviceId,
                    DeviceType.SPEED_CAMERA,
                    DeviceStatus.ACTIVE,
                    new LocationResponseDto("SITE-" + deviceId, "IRAN", "TEHRAN", 35.6892f, 51.3890f));

            deviceRecords.add(dto);
            mockDeviceController.addNewMockDevice(dto);
        }
    }

    private void awaitSync(DeviceCache cache) {
        Awaitility.await()
                .atMost(Duration.ofMillis(2000))
                .pollDelay(Duration.ofMillis(200))
                .until(() -> cache.getTotalDevices() == deviceRecords.size());
    }
}
