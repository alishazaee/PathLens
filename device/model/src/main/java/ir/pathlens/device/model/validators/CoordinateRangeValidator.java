package ir.pathlens.device.model.validators;

import ir.pathlens.device.model.DeviceFilter;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

/**
 * Validates that the min coordinate values are smaller than the max values.
 */
public class CoordinateRangeValidator implements
        ConstraintValidator<ValidCoordinateRange, DeviceFilter> {

    @Override
    public boolean isValid(DeviceFilter filter, ConstraintValidatorContext context) {

        if (filter == null) {
            return true;
        }

        if (filter.minLatitude() != null && filter.maxLatitude() != null) {
            if (filter.minLatitude() > filter.maxLatitude()) {
                return false;
            }
        }

        if (filter.minLongitude() != null && filter.maxLongitude() != null) {
            if (filter.minLongitude() > filter.maxLongitude()) {
                return false;
            }
        }

        return true;
    }
}
