package ir.pathlens.device.rest.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import ir.pathlens.common.model.Page;
import ir.pathlens.device.model.ApiPathConstants;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.model.DeviceStatus;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight mock HTTP server that mimics the device REST API. Stores devices in an in-memory map.
 */
public class MockDeviceController implements AutoCloseable {

    private final HttpServer server;
    private final Map<Integer, DeviceResponseDto> devices = new ConcurrentHashMap<>();
    private final AtomicLong revisionNumber = new AtomicLong(0);
    private final ObjectMapper objectMapper;

    public MockDeviceController(int port) throws IOException {
        objectMapper = new ObjectMapper();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext(ApiPathConstants.GET_REVISION_NUMBER, this::handleGetRevision);
        server.createContext(ApiPathConstants.GET_DEVICES_PATH, this::handleDevices);
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

    private void handleGetRevision(HttpExchange exchange) throws IOException {
        sendJson(exchange, revisionNumber.get());
    }

    private void handleDevices(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (ApiPathConstants.GET_DEVICES_PATH.equals(path)) {
            handleGetDevices(exchange);
            return;
        }
        String idPrefix = ApiPathConstants.GET_DEVICES_PATH + "/";
        if (path.startsWith(idPrefix)) {
            handleGetDevice(exchange, path.substring(idPrefix.length()));
            return;
        }
        sendNotFound(exchange);
    }

    private void handleGetDevices(HttpExchange exchange) throws IOException {
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
    }

    private void handleGetDevice(HttpExchange exchange, String idPath) throws IOException {
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
