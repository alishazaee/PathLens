package ir.pathlens.device.model.validators;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Validates that the min coordinate values are smaller than the max values.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = CoordinateRangeValidator.class)
@Documented
public @interface ValidCoordinateRange {

    String message() default "Invalid coordinate range: min values must be smaller than max values";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
