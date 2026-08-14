package ir.pathlens.device.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ir.pathlens.common.model.Page;
import ir.pathlens.device.client.ApiCallException;
import ir.pathlens.device.client.DeviceClient;
import ir.pathlens.device.model.LocationCreateDto;
import ir.pathlens.device.model.LocationResponseDto;
import ir.pathlens.extension.postgresql.PostgresqlExtension;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.web.server.LocalServerPort;

@ExtendWith(PostgresqlExtension.class)
class LocationControllerTest extends BaseControllerTest {

    private static final String COUNTRY = "IRAN";
    private static final String CITY = "TEHRAN";

    @LocalServerPort
    private int port;

    private DeviceClient client;

    @BeforeEach
    void setup() throws ApiCallException {
        cleanDatabase();
        client = new DeviceClient("http://localhost:" + port);
    }

    @Test
    void shouldCreateLocationSuccessfully() throws ApiCallException {
        LocationCreateDto request = new LocationCreateDto("SITE-1", COUNTRY, CITY, 35.68f, 51.38f);

        LocationResponseDto location = client.createNewLocation(request);

        assertEquals("SITE-1", location.site());
        assertEquals(COUNTRY, location.country());
        assertEquals(CITY, location.city());
        assertEquals(35.68f, location.latitude());
        assertEquals(51.38f, location.longitude());
    }

    @Test
    void shouldGetLocationBySiteId() throws ApiCallException {
        client.createNewLocation(new LocationCreateDto("SITE-2", COUNTRY, CITY, 10.0f, 20.0f));

        LocationResponseDto location = client.getLocation("SITE-2");

        assertEquals("SITE-2", location.site());
        assertEquals(10.0f, location.latitude());
        assertEquals(20.0f, location.longitude());
    }

    @Test
    void shouldGetLocationsPaginated() throws ApiCallException {
        client.createNewLocation(new LocationCreateDto("SITE-3", COUNTRY, CITY, 1.0f, 1.0f));
        client.createNewLocation(new LocationCreateDto("SITE-4", COUNTRY, CITY, 2.0f, 2.0f));
        client.createNewLocation(new LocationCreateDto("SITE-5", COUNTRY, CITY, 3.0f, 3.0f));

        Page<LocationResponseDto> page = client.getLocations(Page.of(List.of(), 0, 2, 0));

        assertEquals(3, page.totalElements());
        assertEquals(2, page.content().size());
        assertEquals(2, page.totalPages());
    }

    @Test
    void shouldFailCreatingDuplicateLocation() throws ApiCallException {
        LocationCreateDto request = new LocationCreateDto("SITE-6", COUNTRY, CITY, 1.0f, 1.0f);
        client.createNewLocation(request);

        assertThrows(ApiCallException.class, () -> client.createNewLocation(request));
    }
}
