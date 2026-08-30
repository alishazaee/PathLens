package ir.pathlens.device.model;

import jakarta.validation.constraints.NotNull;

/**
 * Request payload for updating an existing device. The serial number is treated as an
 * immutable business key and cannot be changed through this endpoint.
 */
public record DeviceUpdateRequestDto(
        @NotNull
        DeviceType type,
        @NotNull
        DeviceStatus status,
        @NotNull
        String siteId) {
}
