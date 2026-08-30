package ir.pathlens.alerting.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.LocalDateTime;

/**
 * Request payload for updating an existing rule.
 */
public record RuleUpdateDto(
        String title,

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

    public RuleUpdateDto {
        if (title == null || title.isEmpty()) {
            title = "NO NAME";
        }
    }
}
