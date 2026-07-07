package ir.pathlens.alerting.rest.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import ir.pathlens.alerting.model.ApiPathConstants;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleType;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lightweight mock HTTP server that mimics the alerting rule REST API. Stores rules in an in-memory list.
 */
public class MockRuleController implements AutoCloseable {

    private final HttpServer server;
    private final List<Rule> rules = new CopyOnWriteArrayList<>();
    private final AtomicInteger revisionNumber = new AtomicInteger(0);
    private final ObjectMapper objectMapper;

    public MockRuleController(int port) throws IOException {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext(ApiPathConstants.GET_ACTIVE_RULES_PATH, this::handleGetActiveRules);
        server.createContext(ApiPathConstants.GET_RULES_REVISION_PATH, this::handleGetRevision);
        server.setExecutor(null);
        server.start();
    }

    public MockRuleController() throws IOException {
        this(0);
    }

    public void addRule(UUID id, String title, String geometryWkt, LocalDateTime expiresAt,
                        IdentityWrapper identity, RuleType ruleType) {
        rules.add(new Rule(id, title, geometryWkt, expiresAt, identity, true, ruleType, false, LocalDateTime.now()));
        revisionNumber.incrementAndGet();
    }

    public void clearRules() {
        rules.clear();
        revisionNumber.incrementAndGet();
    }

    public String getBaseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handleGetActiveRules(HttpExchange exchange) throws IOException {
        byte[] response = objectMapper.writeValueAsBytes(rules);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    private void handleGetRevision(HttpExchange exchange) throws IOException {
        byte[] response = Integer.toString(revisionNumber.get()).getBytes();
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }
}
