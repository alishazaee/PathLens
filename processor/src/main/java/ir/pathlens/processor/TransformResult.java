package ir.pathlens.processor;

import ir.pathlens.proto.CameraLogProto.Error;
import ir.pathlens.proto.CameraLogProto.ErrorType;
import java.util.ArrayList;
import java.util.List;

/**
 * Mutable result of transforming a raw camera log.
 */
public class TransformResult {
    private final List<Error> errors = new ArrayList<>();
    private byte[] log;
    private ErrorType errorType;
    private boolean locationEnriched;
    private boolean parsable;

    public TransformResult addError(Error error) {
        this.errors.add(error);
        return this;
    }

    public TransformResult addErrors(List<Error> errors) {
        this.errors.addAll(errors);
        return this;
    }

    public TransformResult setLog(byte[] log) {
        this.log = log;
        return this;
    }

    public TransformResult setErrorType(ErrorType errorType) {
        this.errorType = errorType;
        return this;
    }

    public TransformResult setLocationEnriched(boolean locationEnriched) {
        this.locationEnriched = locationEnriched;
        return this;
    }

    public TransformResult setParsable(boolean parsable) {
        this.parsable = parsable;
        return this;
    }

    public List<Error> getErrors() {
        return errors;
    }

    public byte[] getLog() {
        return log;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public boolean isLocationEnriched() {
        return locationEnriched;
    }

    public boolean isParsable() {
        return parsable;
    }

    @Override
    public String toString() {
        return "TransformResult{"
                + "errors=" + errors
                + ", errorType=" + errorType
                + ", locationEnriched=" + locationEnriched
                + ", parsable=" + parsable + '}';
    }
}
