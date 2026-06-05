package ir.pathlens.alerting.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.LocalDateTime;

/**
 * Request payload for creating rules.
 */
public record RuleCreateDto(
        String title,

        //TODO add validation for geometry
        @NotBlank(message = "Geometry WKT is required")
        String geometryWkt,

        @NotNull(message = "ExpiresAt is required")
        @Future(message = "expiresAt must be in the future")
        LocalDateTime expiresAt,

        @Valid
        @NotNull(message = "Identity is required")
        IdentityWrapper identity,

        @NotNull
        RuleType ruleType
) {

    public RuleCreateDto {
        if (title.isEmpty()) {
            title = "NO NAME";
        }
    }
}
