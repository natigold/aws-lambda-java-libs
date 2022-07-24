package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import org.reactivestreams.Publisher;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;

import java.nio.ByteBuffer;

public class NextRequestHandler implements SdkAsyncHttpResponseHandler {

    private static final String REQUEST_ID_HEADER = "lambda-runtime-aws-request-id";
    private static final String FUNCTION_ARN_HEADER = "lambda-runtime-invoked-function-arn";
    private static final String DEADLINE_MS_HEADER = "lambda-runtime-deadline-ms";
    private static final String TRACE_ID_HEADER = "lambda-runtime-trace-id";
    private static final String CLIENT_CONTEXT_HEADER = "lambda-runtime-client-context";
    private static final String COGNITO_IDENTITY_HEADER = "lambda-runtime-cognito-identity";
    private static final BodyByteBufferSubscriber BODY_BYTEBUFFER_SUBSCRIBER = new BodyByteBufferSubscriber();

    private SdkHttpResponse headers;
    private Throwable error;

    @Override
    public void onHeaders(SdkHttpResponse headers) {
        this.headers = headers;
    }

    @Override
    public void onStream(Publisher<ByteBuffer> stream) {
        stream.subscribe(BODY_BYTEBUFFER_SUBSCRIBER);
    }

    @Override
    public void onError(Throwable t) {
        error = t;
    }

    public InvocationRequest getInvocationRequest() {

        if (error != null) {
            throw new LambdaRuntimeClientException("Failed to get response", error);
        }

        InvocationRequest result = new InvocationRequest();

        result.content = BODY_BYTEBUFFER_SUBSCRIBER.getPaload();

        result.id = headers.firstMatchingHeader(REQUEST_ID_HEADER).orElseThrow(
                () -> new LambdaRuntimeClientException("Request ID absent"));
        result.invokedFunctionArn = headers.firstMatchingHeader(FUNCTION_ARN_HEADER).orElseThrow(
                () -> new LambdaRuntimeClientException("Function ARN absent"));
        result.deadlineTimeInMs = Long.parseLong(headers.firstMatchingHeader(DEADLINE_MS_HEADER).orElse("0"));
        result.xrayTraceId = headers.firstMatchingHeader(TRACE_ID_HEADER).orElse(null);
        result.clientContext = headers.firstMatchingHeader(CLIENT_CONTEXT_HEADER).orElse(null);
        result.cognitoIdentity = headers.firstMatchingHeader(COGNITO_IDENTITY_HEADER).orElse(null);

        return result;
    }
}
