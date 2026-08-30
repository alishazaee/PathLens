package ir.pathlens.common.model;

import java.time.Instant;
import java.util.List;

/**
 * A consistent error payload returned by every REST API on failure.
 *
 * @param timestamp    when the error occurred
 * @param status       the HTTP status code
 * @param error        the HTTP status reason phrase
 * @param message      a human-readable description of the failure
 * @param path         the request path that failed
 * @param fieldErrors  field-level validation failures, empty when not applicable
 */
public record ErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String message,
        String path,
        List<FieldError> fieldErrors
) {

    public ErrorResponse {
        fieldErrors = fieldErrors == null ? List.of() : fieldErrors;
    }

    public static ErrorResponse of(int status, String error, String message, String path) {
        return new ErrorResponse(Instant.now(), status, error, message, path, List.of());
    }

    public static ErrorResponse ofFieldErrors(int status, String error, String path, List<FieldError> fieldErrors) {
        return new ErrorResponse(Instant.now(), status, error, "Validation failed", path, fieldErrors);
    }

    /**
     * A single field validation failure.
     *
     * @param field   the name of the invalid field
     * @param message the reason it is invalid
     */
    public record FieldError(String field, String message) {
    }
}
