package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import software.amazon.awssdk.crt.http.HttpHeader;
import software.amazon.awssdk.crt.http.HttpStreamBase;
import software.amazon.awssdk.crt.http.HttpStreamBaseResponseHandler;

public class CrtStreamResponseHandler implements HttpStreamBaseResponseHandler {
    private static final String REQUEST_ID_HEADER = "Lambda-Runtime-Aws-Request-Id";
    private static final String FUNCTION_ARN_HEADER = "Lambda-Runtime-Invoked-Function-Arn";
    private static final String DEADLINE_MS_HEADER = "Lambda-Runtime-Deadline-Ms";
    private static final String TRACE_ID_HEADER = "Lambda-Runtime-Trace-Id";
    private static final String CLIENT_CONTEXT_HEADER = "Lambda-Runtime-Client-Context";
    private static final String COGNITO_IDENTITY_HEADER = "Lambda-Runtime-Cognito-Identity";

    int statusCode = -1;
    int blockType = -1;
    List<HttpHeader> headers = new ArrayList<>();
    ByteBuffer bodyBuffer = ByteBuffer.wrap(new byte[16 * 1024 * 1024]); // Allow up to 16 MB Responses
    final CompletableFuture<Void> completableFuture;

    int onCompleteErrorCode = -1;

    public CrtStreamResponseHandler(CompletableFuture<Void> completableFuture) {
        this.completableFuture = completableFuture;
        bodyBuffer.clear(); // Clear the buffer before first write
    }

    public String getBody() {
        bodyBuffer.flip();
        return StandardCharsets.UTF_8.decode(bodyBuffer).toString();
    }

    public byte[] getBodyBytes() {
        bodyBuffer.flip();
        byte[] arr = new byte[bodyBuffer.remaining()];
        bodyBuffer.get(arr);  
        return arr;
    }

    @Override
    public void onResponseHeaders(HttpStreamBase stream, int responseStatusCode, int blockType,
            HttpHeader[] nextHeaders) {
        this.statusCode = responseStatusCode;
        this.headers.addAll(Arrays.asList(nextHeaders));
    }

    @Override
    public void onResponseHeadersDone(HttpStreamBase stream, int blockType) {
        this.blockType = blockType;
    }

    @Override
    public int onResponseBody(HttpStreamBase stream, byte[] bodyBytesIn) {
        this.bodyBuffer.put(bodyBytesIn);
        int amountRead = bodyBytesIn.length;

        // Slide the window open by the number of bytes just read
        return amountRead;
    }

    @Override
    public void onResponseComplete(HttpStreamBase stream, int errorCode) {
        this.onCompleteErrorCode = errorCode;
        completableFuture.complete(null);
    }

    public InvocationRequest getInvocationRequest() {
        InvocationRequest request = new InvocationRequest();

        request.setContent(getBodyBytes());

        request.id = null;
        request.invokedFunctionArn = null;
        request.deadlineTimeInMs = 0;
        request.xrayTraceId = null;
        request.clientContext = null;
        request.cognitoIdentity = null;

        this.headers.stream().forEach(header -> {
            switch (header.getName()) {
                case REQUEST_ID_HEADER:
                    request.id = header.getValue();
                    break;
            
                case FUNCTION_ARN_HEADER:
                    request.invokedFunctionArn = header.getValue();
                    break;
            
                case DEADLINE_MS_HEADER:
                    request.deadlineTimeInMs = Long.parseLong(header.getValue());
                    break;
            
                case TRACE_ID_HEADER:
                    request.xrayTraceId = header.getValue();
                    break;
            
                case CLIENT_CONTEXT_HEADER:
                    request.clientContext = header.getValue();
                    break;
            
                case COGNITO_IDENTITY_HEADER:
                    request.cognitoIdentity = header.getValue();
                    break;
            
                default:
                    break;
            }
        });

        Optional.ofNullable(request.id)
                .orElseThrow(() -> new LambdaRuntimeClientException("Request ID absent"));
        Optional.ofNullable(request.invokedFunctionArn)
                .orElseThrow(() -> new LambdaRuntimeClientException("Function ARN absent"));

        return request;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Status: " + statusCode);
        int i = 0;
        for (HttpHeader h : headers) {
            builder.append("\nHeader[" + i + "]: " + h.toString());
        }

        builder.append("\nBody:\n");
        builder.append(getBody());

        return builder.toString();
    }
}
