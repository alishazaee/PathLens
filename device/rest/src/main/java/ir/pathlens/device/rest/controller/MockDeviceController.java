package ir.pathlens.device.rest.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import ir.pathlens.common.model.Page;
import ir.pathlens.device.model.ApiPathConstants;
import ir.pathlens.device.model.DeviceCreateRequestDto;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.LocationCreateDto;
import ir.pathlens.device.model.LocationResponseDto;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight mock HTTP server that mimics the device REST API. Stores devices and locations in
 * in-memory maps.
 */
public class MockDeviceController implements AutoCloseable {

    private final HttpServer server;
    private final Map<Integer, DeviceResponseDto> devices = new ConcurrentHashMap<>();
    private final Map<String, LocationResponseDto> locations = new ConcurrentHashMap<>();
    private final AtomicLong revisionNumber = new AtomicLong(0);
    private final ObjectMapper objectMapper;
    private final AtomicInteger nextDeviceId = new AtomicInteger(1);

    public MockDeviceController(int port) throws IOException {
        objectMapper = new ObjectMapper();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext(ApiPathConstants.GET_REVISION_NUMBER, this::getRevision);
        server.createContext(ApiPathConstants.CREATE_DEVICE_PATH, this::handleDevices);
        server.createContext(ApiPathConstants.CREATE_LOCATIONS_PATH, this::handleLocations);
        server.setExecutor(null);
        server.start();
    }

    public MockDeviceController() throws IOException {
        this(0);
    }

    public void addNewMockDevice(DeviceResponseDto deviceResponseDto) {
        devices.put(deviceResponseDto.id(), deviceResponseDto);
        revisionNumber.incrementAndGet();
    }

    public String getBaseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void getRevision(HttpExchange exchange) throws IOException {
        sendJson(exchange, revisionNumber.get());
    }

    private void handleDevices(HttpExchange exchange) throws IOException {
        if ("POST".equals(exchange.getRequestMethod())) {
            createDevice(exchange);
            return;
        }
        getDevices(exchange);
    }

    private void handleLocations(HttpExchange exchange) throws IOException {
        if ("POST".equals(exchange.getRequestMethod())) {
            createLocation(exchange);
            return;
        }
        getLocations(exchange);
    }

    private void createDevice(HttpExchange exchange) throws IOException {
        DeviceCreateRequestDto request = objectMapper.readValue(exchange.getRequestBody(),
                DeviceCreateRequestDto.class);
        LocationResponseDto location = locations.get(request.siteId());
        int id = nextDeviceId.getAndIncrement();
        DeviceResponseDto device = new DeviceResponseDto(id, request.serialNumber(), request.type(),
                request.status(), location);
        devices.put(id, device);
        revisionNumber.incrementAndGet();
        sendJson(exchange, device);
    }

    private void createLocation(HttpExchange exchange) throws IOException {
        LocationCreateDto locationRequest = objectMapper.readValue(exchange.getRequestBody(),
                LocationCreateDto.class);
        LocationResponseDto location = new LocationResponseDto(locationRequest.site(),
                locationRequest.country(), locationRequest.city(), locationRequest.latitude(),
                locationRequest.longitude());
        locations.put(location.site(), location);
        revisionNumber.incrementAndGet();
        sendJson(exchange, location);
    }

    private void getDevices(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (ApiPathConstants.GET_DEVICES_PATH.equals(path)) {
            Map<String, String> query = parseQuery(exchange.getRequestURI().getQuery());
            int page = Integer.parseInt(query.getOrDefault("page", "0"));
            int size = Integer.parseInt(query.getOrDefault("size", "10"));
            boolean justActive = Boolean.parseBoolean(query.getOrDefault("justActiveDevices", "false"));

            List<DeviceResponseDto> filtered = devices.values().stream()
                    .filter(device -> !justActive || device.status() == DeviceStatus.ACTIVE)
                    .sorted(Comparator.comparingInt(DeviceResponseDto::id))
                    .toList();

            long start = (long) page * size;
            List<DeviceResponseDto> content = start >= filtered.size()
                    ? List.of()
                    : filtered.subList((int) start, (int) Math.min(start + size, filtered.size()));
            int totalPages = size == 0 ? 0 : (int) Math.ceil((double) filtered.size() / size);

            sendJson(exchange, new Page<>(content, page, size, filtered.size(), totalPages));

            return;
        }
        String idPrefix = ApiPathConstants.GET_DEVICES_PATH + "/";
        if (path.startsWith(idPrefix)) {
            getDevice(exchange, path.substring(idPrefix.length()));
            return;
        }
        sendNotFound(exchange);
    }

    private void getDevice(HttpExchange exchange, String idPath) throws IOException {
        try {
            DeviceResponseDto device = devices.get(Integer.parseInt(idPath));
            if (device == null) {
                sendNotFound(exchange);
                return;
            }
            sendJson(exchange, device);
        } catch (NumberFormatException e) {
            sendNotFound(exchange);
        }
    }

    private void getLocations(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String idPrefix = ApiPathConstants.GET_LOCATIONS_PATH + "/";
        if (path.startsWith(idPrefix)) {
            getLocation(exchange, path.substring(idPrefix.length()));
            return;
        }

        Map<String, String> query = parseQuery(exchange.getRequestURI().getQuery());
        int page = Integer.parseInt(query.getOrDefault("page", "0"));
        int size = Integer.parseInt(query.getOrDefault("size", "10"));

        List<LocationResponseDto> all = locations.values().stream()
                .sorted(Comparator.comparing(LocationResponseDto::site))
                .toList();

        long start = (long) page * size;
        List<LocationResponseDto> content = start >= all.size()
                ? List.of()
                : all.subList((int) start, (int) Math.min(start + size, all.size()));
        int totalPages = size == 0 ? 0 : (int) Math.ceil((double) all.size() / size);

        sendJson(exchange, new Page<>(content, page, size, all.size(), totalPages));
    }

    private void getLocation(HttpExchange exchange, String siteId) throws IOException {
        LocationResponseDto location = locations.get(siteId);
        if (location == null) {
            sendNotFound(exchange);
            return;
        }
        sendJson(exchange, location);
    }

    private void sendJson(HttpExchange exchange, Object body) throws IOException {
        byte[] response = objectMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    private void sendNotFound(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(404, -1);
    }

    private static Map<String, String> parseQuery(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isEmpty()) {
            return params;
        }
        for (String pair : query.split("&")) {
            String[] keyValue = pair.split("=", 2);
            String key = URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8);
            String value = keyValue.length > 1 ? URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8) : "";
            params.put(key, value);
        }
        return params;
    }
}
