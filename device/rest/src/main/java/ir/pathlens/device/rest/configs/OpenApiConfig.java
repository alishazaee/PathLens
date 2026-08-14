package ir.pathlens.device.rest.configs;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI documentation configuration for the device REST API.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI deviceApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Device REST API")
                        .version("1.0")
                        .description("REST API for device and location management"));
    }
}

