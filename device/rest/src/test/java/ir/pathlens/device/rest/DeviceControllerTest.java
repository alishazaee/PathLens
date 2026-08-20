package ir.pathlens.device.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.common.model.Page;
import ir.pathlens.device.cache.DeviceCache;
import ir.pathlens.device.client.ApiCallException;
import ir.pathlens.device.client.DeviceClient;
import ir.pathlens.device.model.DeviceCreateRequestDto;
import ir.pathlens.device.model.DeviceFilter;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.DeviceType;
import ir.pathlens.device.model.LocationCreateDto;
import ir.pathlens.extension.postgresql.PostgresqlExtension;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.web.server.LocalServerPort;

@ExtendWith(PostgresqlExtension.class)
class DeviceControllerTest extends BaseControllerTest {


    @LocalServerPort
    private int port;

    private DeviceClient client;

    @BeforeEach
    void setup() throws ApiCallException {
        cleanDatabase();
        client = new DeviceClient("http://localhost:" + port);
    }

    @Test
    void shouldCreateDeviceSuccessfully() throws ApiCallException {
        createLocation("SITE-1");
        DeviceResponseDto device = createDevice("DEV-1", "SITE-1");

        assertEquals("DEV-1", device.serialNumber());
        assertEquals(DeviceType.SPEED_CAMERA, device.deviceType());
        assertEquals(DeviceStatus.ACTIVE, device.status());
        assertEquals("SITE-1", device.deviceLocationDto().site());

        DeviceResponseDto fetched = client.getDevice(device.id());
        assertEquals(device, fetched);
    }

    @Test
    void shouldFailCreatingDuplicateDevice() throws ApiCallException {
        createLocation("SITE-2");
        createDevice("DEV-2", "SITE-2");

        assertThrows(ApiCallException.class, () -> createDevice("DEV-2", "SITE-2"));
    }

    @Test
    void shouldFailCreatingDeviceWithUnknownLocation() {
        assertThrows(ApiCallException.class, () -> createDevice("DEV-3", "UNKNOWN-SITE"));
    }

    @Test
    void shouldGetDevicesPaginatedAndFiltered() throws ApiCallException {
        createLocation("SITE-3");
        createDevice("DEV-4", "SITE-3", DeviceType.SPEED_CAMERA, DeviceStatus.ACTIVE);
        createDevice("DEV-5", "SITE-3", DeviceType.TOLL_CAMERA, DeviceStatus.INACTIVE);
        createDevice("DEV-6", "SITE-3", DeviceType.RED_LIGHT_CAMERA, DeviceStatus.ACTIVE);

        Page<DeviceResponseDto> firstPage = client.getDevices(null, Page.of(List.of(), 0, 2, 0));

        assertEquals(3, firstPage.totalElements());
        assertEquals(2, firstPage.content().size());

        DeviceFilter bySerial = new DeviceFilter(null, "DEV-5", null, null, null, null, null);
        Page<DeviceResponseDto> bySerialPage = client.getDevices(bySerial, Page.of(List.of(), 0, 10, 0));

        assertEquals(1, bySerialPage.totalElements());
        assertEquals("DEV-5", bySerialPage.content().get(0).serialNumber());

        DeviceFilter justActive = new DeviceFilter(true, null, null, null, null, null, null);
        Page<DeviceResponseDto> activePage = client.getDevices(justActive, Page.of(List.of(), 0, 10, 0));

        assertEquals(2, activePage.totalElements());
    }

    @Test
    void shouldGetRevisionNumber() throws ApiCallException {
        assertEquals(0L, client.getRevisionNumber().longValue());

        createLocation("SITE-4");
        createDevice("DEV-7", "SITE-4");

        assertNotEquals(0L, client.getRevisionNumber().longValue());
    }

    @Test
    void shouldDeleteDeviceSuccessfully() throws ApiCallException {
        createLocation("SITE-5");
        DeviceResponseDto device = createDevice("DEV-8", "SITE-5");

        client.deleteDevice(device.id());

        assertThrows(ApiCallException.class, () -> client.getDevice(device.id()));
        assertEquals(0, client.getDevices(null, Page.of(List.of(), 0, 10, 0)).totalElements());
    }

    @Test
    void shouldSyncDeviceCacheFromServer() throws ApiCallException {
        createLocation("SITE-6");
        createDevice("DEV-9", "SITE-6", DeviceType.SPEED_CAMERA, DeviceStatus.ACTIVE);
        createDevice("DEV-10", "SITE-6", DeviceType.SPEED_CAMERA, DeviceStatus.ACTIVE);

        try (DeviceCache deviceCache = new DeviceCache(client, 10, 1000, 1000)) {
            deviceCache.sync();

            assertTrue(deviceCache.findByDeviceSerialNumber("DEV-9").isPresent());
            assertTrue(deviceCache.findByDeviceSerialNumber("DEV-10").isPresent());
            assertEquals(2, deviceCache.getTotalDevices());
            assertEquals(client.getRevisionNumber().longValue(), deviceCache.getRevisionNumber());
        }
    }

    private void createLocation(String site) throws ApiCallException {
        client.createNewLocation(new LocationCreateDto(site, "IRAN", "TEHRAN", 10.21f, -10.2f));
    }

    private DeviceResponseDto createDevice(String serial, String site) throws ApiCallException {
        return createDevice(serial, site, DeviceType.SPEED_CAMERA, DeviceStatus.ACTIVE);
    }

    private DeviceResponseDto createDevice(String serial, String site, DeviceType type, DeviceStatus status)
            throws ApiCallException {
        return client.createNewDevice(new DeviceCreateRequestDto(serial, type, status, site));
    }
}
