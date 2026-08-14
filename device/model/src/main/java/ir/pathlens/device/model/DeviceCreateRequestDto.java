package ir.pathlens.device.model;

import jakarta.validation.constraints.NotNull;

/**
 * Data transfer object representing device information
 * exposed through APIs.
 */
public record DeviceCreateRequestDto(
        String serialNumber,
        DeviceType type,
        DeviceStatus status,
        @NotNull
        String siteId){
}
