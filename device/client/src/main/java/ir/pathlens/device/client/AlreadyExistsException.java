package ir.pathlens.device.client;

public class AlreadyExistsException extends ApiCallException{
    public AlreadyExistsException(String message) {
        super(message);
    }

}
