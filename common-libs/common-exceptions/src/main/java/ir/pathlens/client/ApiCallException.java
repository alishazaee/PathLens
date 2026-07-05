package ir.pathlens.client;

/**
 * Exception thrown when an API call fails.
 */
public class ApiCallException extends Exception {
    private static final long serialVersionUID = 1L;

    public ApiCallException(String message) {
        super(message);
    }
}
