package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import java.util.Optional;

public class InvocationError {

    /**
     * The Lambda request ID associated with the request.
     */
    private String id;

    /**
     * Error response
     */
    private byte[] errorResponse;

    /**
     * Lambda-Runtime-Function-Error-Type
     */
    private String errorType;

    /*
     * Lambda-Runtime-Function-XRay-Error-Cause
     */
    private String errorCause;

    public String getId() {
        return id;
    }

    public byte[] getErrorResponse() {
        return errorResponse;
    }

    public Optional<String> getErrorType() {
        return Optional.ofNullable(errorType);
    }

    public Optional<String> getErrorCause() {
        return Optional.ofNullable(errorCause);
    }

    public static InvocationErrorBuilder newBuilder(byte[] errorResponse) {
        return new InvocationErrorBuilder(errorResponse);
    }

    public static class InvocationErrorBuilder {
        private static final int XRAY_ERROR_CAUSE_MAX_HEADER_SIZE = 1024 * 1024; // 1MiB
        InvocationError result;

        InvocationErrorBuilder(byte[] errorResponse) {
            result = new InvocationError();
            result.errorResponse = errorResponse;
        }

        public InvocationErrorBuilder setId(String id) {
            result.id = id;
            return this;
        }

        public InvocationErrorBuilder setErrorType(String errorType) {
            if (errorType != null && !errorType.isEmpty()) {
                result.errorType = errorType;
            }
            return this;
        }

        public InvocationErrorBuilder setErrorCause(String errorCause) {
            if (errorCause != null && !errorCause.isEmpty()
                    && errorCause.getBytes().length < XRAY_ERROR_CAUSE_MAX_HEADER_SIZE) {
                result.errorCause = errorCause;
            }
            return this;
        }

        public InvocationError build() {
            return result;
        }
    }
}
