package ir.pathlens.device.model;

/**
 * Dto for device locations.
 */
public record LocationResponseDto(
        String site,
        String country,
        String city,
        Float latitude,
        Float longitude
) {
}