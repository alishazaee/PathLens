package ir.pathlens.device.model;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;

/**
 * Request payload for updating an existing location. The site id is treated as an immutable
 * business key and cannot be changed through this endpoint.
 */
public record LocationUpdateDto(
        String country,
        String city,
        @DecimalMin(value = "-90.0", message = "latitude must be between -90 and 90")
        @DecimalMax(value = "90.0", message = "latitude must be between -90 and 90")
        Float latitude,
        @DecimalMin(value = "-180.0", message = "longitude must be between -180 and 180")
        @DecimalMax(value = "180.0", message = "longitude must be between -180 and 180")
        Float longitude
) {
}
