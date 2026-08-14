package ir.pathlens.device.client;

/**
 * Thrown when the device REST API returns an unexpected response.
 */
public class ApiCallException extends Exception {
    private static final long serialVersionUID = 1L;

    public ApiCallException(String message) {
        super(message);
    }

    public ApiCallException(String message, Exception e) {
        super(message, e);
    }
}
