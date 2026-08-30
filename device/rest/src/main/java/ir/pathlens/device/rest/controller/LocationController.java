package ir.pathlens.device.rest.controller;

import ir.pathlens.common.model.Page;
import ir.pathlens.device.model.ApiPathConstants;
import ir.pathlens.device.model.LocationCreateDto;
import ir.pathlens.device.model.LocationResponseDto;
import ir.pathlens.device.model.LocationUpdateDto;
import ir.pathlens.device.rest.service.LocationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for device location management.
 */
@RestController
@RequiredArgsConstructor
public class LocationController {

    private final LocationService locationService;

    @PostMapping(ApiPathConstants.CREATE_LOCATIONS_PATH)
    @ResponseStatus(HttpStatus.CREATED)
    public LocationResponseDto createLocation(@RequestBody @Valid LocationCreateDto request) {
        return locationService.createLocation(request);
    }

    @GetMapping(ApiPathConstants.GET_LOCATION_PATH)
    public LocationResponseDto getLocation(@PathVariable String siteId) {
        return locationService.getLocation(siteId);
    }

    @GetMapping(ApiPathConstants.GET_LOCATIONS_PATH)
    public Page<LocationResponseDto> getLocations(@ParameterObject Pageable pageable) {
        return locationService.getPaginatedLocations(pageable);
    }

    @PutMapping(ApiPathConstants.UPDATE_LOCATION_PATH)
    public LocationResponseDto updateLocation(
            @PathVariable String siteId, @RequestBody @Valid LocationUpdateDto request) {
        return locationService.updateLocation(siteId, request);
    }

    @DeleteMapping(ApiPathConstants.DELETE_LOCATION_PATH)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteLocation(@PathVariable String siteId) {
        locationService.deleteLocation(siteId);
    }
}

