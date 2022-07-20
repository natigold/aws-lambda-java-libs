package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

/**
 * Copyright (c) 2019 Amazon. All rights reserved.
 */
public class LambdaRuntimeClientException extends RuntimeException {

    public LambdaRuntimeClientException() {
        super();
    }

    public LambdaRuntimeClientException(String message) {
        super(message);
    }

    public LambdaRuntimeClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public LambdaRuntimeClientException(Throwable cause) {
        super(cause);
    }
    
    public LambdaRuntimeClientException(String message, int responseCode) {
        super(message + "Response code: '" + responseCode + "'.");
    }
}
