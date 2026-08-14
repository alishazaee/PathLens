package ir.pathlens.device.client;

import static ir.pathlens.device.model.ApiPathConstants.CREATE_DEVICE_PATH;
import static ir.pathlens.device.model.ApiPathConstants.CREATE_LOCATIONS_PATH;
import static ir.pathlens.device.model.ApiPathConstants.DELETE_DEVICE_PATH;
import static ir.pathlens.device.model.ApiPathConstants.GET_DEVICES_PATH;
import static ir.pathlens.device.model.ApiPathConstants.GET_DEVICE_PATH;
import static ir.pathlens.device.model.ApiPathConstants.GET_LOCATIONS_PATH;
import static ir.pathlens.device.model.ApiPathConstants.GET_LOCATION_PATH;
import static ir.pathlens.device.model.ApiPathConstants.GET_REVISION_NUMBER;
import static ir.pathlens.device.model.ApiPathConstants.buildPath;
import static jakarta.ws.rs.core.Response.Status.Family.SUCCESSFUL;

import com.fasterxml.jackson.core.util.JacksonFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import ir.pathlens.common.model.Page;
import ir.pathlens.device.model.DeviceCreateRequestDto;
import ir.pathlens.device.model.DeviceFilter;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.model.LocationCreateDto;
import ir.pathlens.device.model.LocationResponseDto;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.TimeUnit;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP client for the device REST API.
 */
public class DeviceClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DeviceClient.class);

    private Client client;
    private final String baseUrl;

    public DeviceClient(String baseUrl) throws ApiCallException {
        this.baseUrl = baseUrl;
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);  // Disable timestamps for better formatting

        JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        provider.setMapper(objectMapper);

        this.client = ClientBuilder.newBuilder()
                .register(JacksonFeature.class)
                .withConfig(new ClientConfig().register(provider))
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    public LocationResponseDto createNewLocation(LocationCreateDto locationCreateDto) throws ApiCallException {
        WebTarget target = client.target(baseUrl).path(buildPath(CREATE_LOCATIONS_PATH));

        try (Response response = target.request()
                .post(Entity.entity(locationCreateDto, MediaType.APPLICATION_JSON_TYPE))) {
            return handleError(response, LocationResponseDto.class);
        } catch (ProcessingException e) {
            throw new ApiCallException("Device API call failed: " + target.getUri(), e);
        }
    }

    public DeviceResponseDto createNewDevice(DeviceCreateRequestDto deviceCreateRequestDto) throws ApiCallException {
        WebTarget target = client.target(baseUrl)
                .path(buildPath(CREATE_DEVICE_PATH));

        try (Response response = target.request()
                .post(Entity.entity(deviceCreateRequestDto, MediaType.APPLICATION_JSON_TYPE))) {
            return handleError(response, DeviceResponseDto.class);
        } catch (ProcessingException e) {
            throw new ApiCallException("Device API call failed: " + target.getUri(), e);
        }
    }

    public LocationResponseDto getLocation(String siteId) throws ApiCallException {
        WebTarget target = client.target(baseUrl).path(buildPath(GET_LOCATION_PATH, siteId));

        try (Response response = target.request().get()) {
            return handleError(response, LocationResponseDto.class);
        } catch (ProcessingException e) {
            throw new ApiCallException("Device API call failed: " + target.getUri(), e);
        }
    }

    public Page<LocationResponseDto> getLocations(Page<?> page) throws ApiCallException {
        WebTarget target = client.target(baseUrl).path(buildPath(GET_LOCATIONS_PATH));
        target = addPaginationParams(page, target);

        try (Response response = target.request().get()) {

            if (response.getStatusInfo().getFamily() == SUCCESSFUL) {
                return response.readEntity(new GenericType<>() {});
            }

            String errMessage = response.readEntity(String.class);
            throw new ApiCallException(errMessage);
        } catch (ProcessingException e) {
            throw new ApiCallException("Device API call failed: " + target.getUri(), e);
        }
    }

    public DeviceResponseDto getDevice(int id) throws ApiCallException {
        WebTarget target = client.target(baseUrl).path(buildPath(GET_DEVICE_PATH, id));

        try (Response response = target.request().get()) {
            return handleError(response, DeviceResponseDto.class);
        } catch (ProcessingException e) {
            throw new ApiCallException("Device API call failed: " + target.getUri(), e);
        }
    }

    public Long getRevisionNumber() throws ApiCallException {
        WebTarget target = client.target(baseUrl).path(buildPath(GET_REVISION_NUMBER));

        try (Response response = target.request().get()) {
            return handleError(response, Long.class);
        } catch (ProcessingException e) {
            throw new ApiCallException("Device API call failed: " + target.getUri(), e);
        }
    }

    public void deleteDevice(int id) throws ApiCallException {
        WebTarget target = client.target(baseUrl).path(buildPath(DELETE_DEVICE_PATH, id));

        try (Response response = target.request().delete()) {
            handleError(response, DeviceResponseDto.class);
        } catch (ProcessingException e) {
            throw new ApiCallException("Device API call failed: " + target.getUri(), e);
        }
    }

    public Page<DeviceResponseDto> getDevices(DeviceFilter filter, Page<?> page) throws ApiCallException {
        WebTarget target = client.target(baseUrl).path(buildPath(GET_DEVICES_PATH));

        target = addFilterParams(filter, target);
        target = addPaginationParams(page, target);

        try (Response response = target.request().get()) {

            if (response.getStatusInfo().getFamily() == SUCCESSFUL) {
                return response.readEntity(new GenericType<>() {});
            }

            String errMessage = response.readEntity(String.class);
            throw new ApiCallException(errMessage);
        } catch (ProcessingException e) {
            throw new ApiCallException("Device API call failed: " + target.getUri(), e);
        }
    }

    private WebTarget addFilterParams(DeviceFilter filter, WebTarget target) {
        if (filter == null) {
            return target;
        }

        if (filter.justActiveDevices() != null) {
            target = target.queryParam("justActiveDevices", filter.justActiveDevices());
        }
        if (filter.minLatitude() != null) {
            target = target.queryParam("minLatitude", filter.minLatitude());
        }
        if (filter.maxLatitude() != null) {
            target = target.queryParam("maxLatitude", filter.maxLatitude());
        }
        if (filter.minLongitude() != null) {
            target = target.queryParam("minLongitude", filter.minLongitude());
        }
        if (filter.maxLongitude() != null) {
            target = target.queryParam("maxLongitude", filter.maxLongitude());
        }
        if (filter.serialNumber() != null) {
            target = target.queryParam("serialNumber", filter.serialNumber());
        }

        return target;
    }

    private WebTarget addPaginationParams(Page<?> page, WebTarget target) {
        if (page == null) {
            return target;
        }

        return target
                .queryParam("page", page.page())
                .queryParam("size", page.size());
    }

    private <T> T handleError(Response response, Class<T> clazz) throws ApiCallException {
        if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
            String errMessage = response.readEntity(String.class);
            logger.error("DeviceCacheClient API error: {}", errMessage);
            throw new ApiCallException(errMessage);
        }
        if (clazz != null) {
            return response.readEntity(clazz);
        }
        return null;
    }

    @Override
    public void close() {
        client.close();
    }
}
