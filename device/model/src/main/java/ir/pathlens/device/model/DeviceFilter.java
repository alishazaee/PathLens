package ir.pathlens.device.model;

import ir.pathlens.device.model.validators.ValidCoordinateRange;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;

/**
 * Filter object used for searching devices with pagination.
 */
@ValidCoordinateRange
public record DeviceFilter(
        Boolean justActiveDevices,

        String serialNumber,

        DeviceType type,

        @DecimalMin(value = "-90.0")
        @DecimalMax(value = "90.0")
        Float minLatitude,

        @DecimalMin(value = "-90.0")
        @DecimalMax(value = "90.0")
        Float maxLatitude,

        @DecimalMin(value = "-180.0")
        @DecimalMax(value = "180.0")
        Float minLongitude,

        @DecimalMin(value = "-180.0")
        @DecimalMax(value = "180.0")
        Float maxLongitude
) {
}
