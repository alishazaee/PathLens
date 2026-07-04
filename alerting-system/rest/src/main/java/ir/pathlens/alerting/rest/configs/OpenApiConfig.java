package ir.pathlens.alerting.rest.configs;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI documentation configuration.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI alertingApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Alerting system API")
                        .version("1.0")
                        .description("API for defining new alerts on users."));
    }
}

