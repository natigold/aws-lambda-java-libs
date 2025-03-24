/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/
package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import com.amazonaws.services.lambda.runtime.api.client.runtimeapi.dto.InvocationRequest;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.crt.http.HttpClientConnection;
import software.amazon.awssdk.crt.http.HttpClientConnectionManager;
import software.amazon.awssdk.crt.http.HttpClientConnectionManagerOptions;
import software.amazon.awssdk.crt.http.HttpHeader;
import software.amazon.awssdk.crt.http.HttpRequest;
import software.amazon.awssdk.crt.http.HttpRequestBodyStream;
import software.amazon.awssdk.crt.http.HttpStream;
import software.amazon.awssdk.crt.http.HttpStreamResponseHandler;
import software.amazon.awssdk.crt.io.ClientBootstrap;
import software.amazon.awssdk.crt.io.SocketOptions;
import software.amazon.awssdk.crt.io.SocketOptions.SocketDomain;

// import software.amazon.awssdk.crt.io.EventLoopGroup;
// import software.amazon.awssdk.crt.io.HostResolver;

/**
 * This module defines the CRT Runtime Interface Client which is responsible for HTTP
 * interactions with the Runtime API.
 */
class CrtClient {

    private static final String INVOCATION_SUCCESS_PATH_TEMPLATE = "/2018-06-01/runtime/invocation/%s/response";
    private static final String DEFAULT_CONTENT_TYPE = "application/json";

    private static final String REQUEST_ID_HEADER = "Lambda-Runtime-Aws-Request-Id";
    private static final String FUNCTION_ARN_HEADER = "Lambda-Runtime-Invoked-Function-Arn";
    private static final String DEADLINE_MS_HEADER = "Lambda-Runtime-Deadline-Ms";
    private static final String TRACE_ID_HEADER = "Lambda-Runtime-Trace-Id";
    private static final String CLIENT_CONTEXT_HEADER = "Lambda-Runtime-Client-Context";
    private static final String COGNITO_IDENTITY_HEADER = "Lambda-Runtime-Cognito-Identity";
    private static final HttpHeader CONTENT_TYPE_HEADER = new HttpHeader("Content-Type", DEFAULT_CONTENT_TYPE);
    private static HttpHeader HOST_HEADER;

    // private static EventLoopGroup eventLoopGroup;
    // private static HostResolver hostResolver;
    private static ClientBootstrap clientBootstrap = new ClientBootstrap(null, null);
    private static HttpClientConnectionManager connectionManager;
    private static HttpClientConnection cachedConnection;

    private static String baseUrl;
    private static String invocationEndpoint;
    private static final Map<String, String> responseHeaders = new HashMap<>();
    
    static void init(String awsLambdaRuntimeApi) {
        baseUrl = "http://" + awsLambdaRuntimeApi;
        invocationEndpoint = baseUrl + "/2018-06-01/runtime/invocation/";

        URI uri = URI.create(invocationEndpoint);
        HOST_HEADER = new HttpHeader("Host", uri.getHost());

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.connectTimeoutMs = 1000;
        socketOptions.keepAlive = true;
        socketOptions.domain = SocketDomain.IPv4;

        try {            
            HttpClientConnectionManagerOptions options = new HttpClientConnectionManagerOptions().
                withClientBootstrap(clientBootstrap).
                withSocketOptions(socketOptions).
                withUri(new URI(invocationEndpoint)).
                withMaxConnections(1); 
        
            connectionManager = HttpClientConnectionManager.create(options);
            cachedConnection = acquireConnection();
        } catch (URISyntaxException e) {
            throw new LambdaRuntimeClientException("Error in URI syntax", 500);
        }
    }

    static synchronized HttpClientConnection acquireConnection() {
        if (cachedConnection == null || cachedConnection.isNull()) {
            cachedConnection = connectionManager.acquireConnection().join();
        }
        return cachedConnection;
    }
    
    
    static InvocationRequest next() {
        HttpClientConnection connection = acquireConnection();
        ByteBuffer responseBody = ByteBuffer.allocateDirect(1024); 

        try {
            HttpHeader[] headers = {
                HOST_HEADER 
            };
            HttpRequest request = new HttpRequest("GET", invocationEndpoint + "next", headers, null);

            CompletableFuture<Void> completed = new CompletableFuture<>();

            HttpStream stream = connection.makeRequest(request, new HttpStreamResponseHandler() {
                @Override
                public void onResponseHeaders(HttpStream stream, int statusCode, int blockType, HttpHeader[] headers) {
                    responseHeaders.clear();
                    for (HttpHeader header : headers) {
                        responseHeaders.put(header.getName(), header.getValue());
                    }

                    if (statusCode != 200) {
                        completed.completeExceptionally(new RuntimeException("Error: " + statusCode));
                    }
                }

                @Override
                public void onResponseHeadersDone(HttpStream stream, int blockType) {
                }

                @Override
                public int onResponseBody(HttpStream stream, byte[] bodyBytesIn) {
                    /* 
                    bytes = reinterpret_cast<const jbyte*>(response.payload.c_str());
                    CHECK_EXCEPTION(env, jArray = env->NewByteArray(response.payload.length()));
                    CHECK_EXCEPTION(env, env->SetByteArrayRegion(jArray, 0, response.payload.length(), bytes));
                    CHECK_EXCEPTION(env, env->SetObjectField(invocationRequest, contentField, jArray));
                    */
                    if (responseBody.remaining() < bodyBytesIn.length) {
                        // Buffer is full, signal to pause reading
                        return 0;
                    }
                    responseBody.put(bodyBytesIn);
                    return responseBody.remaining();
                }

                @Override
                public void onResponseComplete(HttpStream stream, int errorCode) {
                    if (errorCode != 0) {
                        completed.completeExceptionally(new RuntimeException("HTTP Error: " + errorCode));
                    } else {
                        completed.complete(null);
                    }
                }
            });

            stream.activate();
            completed.join();

            InvocationRequest invocationRequest = new InvocationRequest();
            invocationRequest.setId(responseHeaders.get(REQUEST_ID_HEADER));
            invocationRequest.setInvokedFunctionArn(responseHeaders.get(FUNCTION_ARN_HEADER));
            invocationRequest.setClientContext(responseHeaders.get(CLIENT_CONTEXT_HEADER));
            invocationRequest.setCognitoIdentity(responseHeaders.get(COGNITO_IDENTITY_HEADER));
            invocationRequest.setInvokedFunctionArn(responseHeaders.get(FUNCTION_ARN_HEADER));
            invocationRequest.setXrayTraceId(responseHeaders.get(TRACE_ID_HEADER));
            
            responseBody.flip();
            byte[] content = new byte[responseBody.remaining()];
            responseBody.get(content);
            invocationRequest.setContent(content);
            responseBody.clear();

            if (Objects.nonNull(responseHeaders.get(DEADLINE_MS_HEADER))) {
                try {
                    invocationRequest.setDeadlineTimeInMs(Long.parseLong(responseHeaders.get(DEADLINE_MS_HEADER)));
                } catch (NullPointerException e) {
                    throw new LambdaRuntimeClientException("Error parsing deadline", 500);                  
                }
            } else {
                invocationRequest.setDeadlineTimeInMs(0);
            }

            return invocationRequest;
        } finally {
            // connection.close();
            boolean bool = false;
        }
    }

    static void postInvocationResponse(byte[] requestId, byte[] responsePayload) {
        sendPostRequest(String.format(INVOCATION_SUCCESS_PATH_TEMPLATE, new String(requestId, StandardCharsets.UTF_8)), responsePayload);
    }

    static void sendPostRequest(String path, byte[] payload) {
        HttpClientConnection connection = acquireConnection();

        try {
            HttpHeader[] headers = {
                HOST_HEADER,
                CONTENT_TYPE_HEADER,
                new HttpHeader("Content-Length", String.valueOf(payload.length))
            };

            HttpRequestBodyStream bodyStream = new HttpRequestBodyStream() {
                private int position = 0;

                @Override
                public boolean sendRequestBody(ByteBuffer outBuffer) {
                    int remaining = payload.length - position;
                    int toWrite = Math.min(outBuffer.remaining(), remaining);
                    outBuffer.put(payload, position, toWrite);
                    position += toWrite;
                    return position < payload.length;
                }
            };

            HttpRequest request = new HttpRequest("POST", baseUrl + path, headers, bodyStream);
            CompletableFuture<Void> completed = new CompletableFuture<>();

            HttpStream stream = connection.makeRequest(request, new HttpStreamResponseHandler() {
                @Override
                public void onResponseHeaders(HttpStream stream, int statusCode, int blockType, HttpHeader[] headers) {
                    if (statusCode != 200 && statusCode != 202) {
                        completed.completeExceptionally(new LambdaRuntimeClientException("postInvocationResponse error code: ", statusCode));
                    }
                }

                @Override
                public void onResponseHeadersDone(HttpStream stream, int blockType) {
                }

                @Override
                public int onResponseBody(HttpStream stream, byte[] bodyBytesIn) {
                    return bodyBytesIn.length;
                }

                @Override
                public void onResponseComplete(HttpStream stream, int errorCode) {
                    if (errorCode != 0) {
                        completed.completeExceptionally(new RuntimeException("HTTP Error: " + errorCode));
                    } else {
                        completed.complete(null);
                    }
                }
            });

            stream.activate();
            completed.join();

        } finally {
            connection.close();
        }
    }
}
