package ir.pathlens.alerting.client;

import static ir.pathlens.alerting.model.ApiPathConstants.GET_ACTIVE_RULES_PATH;
import static ir.pathlens.alerting.model.ApiPathConstants.GET_RULES_REVISION_PATH;
import static ir.pathlens.alerting.model.ApiPathConstants.buildPath;
import static jakarta.ws.rs.core.Response.Status.Family.SUCCESSFUL;

import com.fasterxml.jackson.core.util.JacksonFeature;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.client.ApiCallException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Client for fetching rules from the alerting REST API.
 */
public class RulesClient implements AutoCloseable {
    private final Client client;
    private final String baseUrl;

    public RulesClient(String baseUrl) {
        this.baseUrl = baseUrl;
        this.client = ClientBuilder.newBuilder()
                .register(JacksonFeature.class)
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .build();
    }

    public List<Rule> getAllActiveRules() throws ApiCallException {
        WebTarget target = client.target(baseUrl).path(buildPath(GET_ACTIVE_RULES_PATH));

        try (Response response = target.request().get()) {

            if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                return response.readEntity(new GenericType<>() {});
            }

            String errMessage = response.readEntity(String.class);
            throw new ApiCallException(errMessage);
        }
    }

    public Long getRevisionNumber() throws ApiCallException {
        WebTarget target = client.target(baseUrl).path(buildPath(GET_RULES_REVISION_PATH));

        try (Response response = target.request().get()) {
            return handleError(response, Long.class);
        }
    }

    private <T> T handleError(Response response, Class<T> clazz) throws ApiCallException {
        if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
            String errMessage = response.readEntity(String.class);
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
