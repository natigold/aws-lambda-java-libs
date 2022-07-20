package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * LambdaRuntimeClient is a client of the AWS Lambda Runtime HTTP API for custom runtimes.
 * <p>
 * API definition can be found at https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html
 * <p>
 * Copyright (c) 2019 Amazon. All rights reserved.
 */
public class LambdaRuntimeClient {

    private final String hostname;
    private final int port;
    private final String invocationEndpoint;
    private final AsyncExecuteRequest nextRequest;

    private static final String INVOCATION_ERROR_URL_TEMPLATE = "http://%s/2018-06-01/runtime/invocation/%s:%d/error";
    private static final String INVOCATION_SUCCESS_URL_TEMPLATE = "http://%s/2018-06-01/runtime/invocation/%s:%d/response";
    private static final String NEXT_URL_TEMPLATE = "http://%s/2018-06-01/runtime/invocation/next";
    private static final String INIT_ERROR_URL_TEMPLATE = "http://%s/2018-06-01/runtime/init/error";

    private static final String DEFAULT_CONTENT_TYPE = "application/json";
    private static final String XRAY_ERROR_CAUSE_HEADER = "Lambda-Runtime-Function-XRay-Error-Cause";
    private static final String ERROR_TYPE_HEADER = "Lambda-Runtime-Function-Error-Type";
    private static final int XRAY_ERROR_CAUSE_MAX_HEADER_SIZE = 1024 * 1024; // 1MiB

    private static final String REQUEST_ID_HEADER = "lambda-runtime-aws-request-id";
    private static final String FUNCTION_ARN_HEADER = "lambda-runtime-invoked-function-arn";
    private static final String DEADLINE_MS_HEADER = "lambda-runtime-deadline-ms";
    private static final String TRACE_ID_HEADER = "lambda-runtime-trace-id";
    private static final String CLIENT_CONTEXT_HEADER = "lambda-runtime-client-context";
    private static final String COGNITO_IDENTITY_HEADER = "lambda-runtime-cognito-identity";

    private static final String USER_AGENT = String.format(
            "aws-lambda-java/%s",
            System.getProperty("java.vendor.version"));

    private static final NextRequestHandler NEXT_REQUEST_HANDLER = new NextRequestHandler();

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(Duration.ofDays(1))
            .build();

    private static final SdkAsyncHttpClient CRT_HTTP_CLIENT = AwsCrtAsyncHttpClient.builder().build();

    public static final EmptyContentPublisher EMPTY_CONTENT_PUBLISHER = new EmptyContentPublisher();

    public LambdaRuntimeClient(String hostnamePort) {
        Objects.requireNonNull(hostnamePort, "hostnamePort cannot be null");
        String[] parts = hostnamePort.split(":");
        this.hostname = parts[0];
        this.port = Integer.parseInt(parts[1]);
        this.invocationEndpoint = invocationEndpoint();
        this.nextRequest = AsyncExecuteRequest.builder()
                .fullDuplex(false)
                .responseHandler(NEXT_REQUEST_HANDLER)
                .requestContentPublisher(EMPTY_CONTENT_PUBLISHER)
                .request(SdkHttpRequest.builder()
                        .uri(URI.create(String.format(NEXT_URL_TEMPLATE, hostnamePort)))
                        .appendHeader("User-Agent", USER_AGENT)
                        .method(SdkHttpMethod.GET)
                        .build())
                .build();
    }

    public InvocationRequest waitForNextInvocation() {
        try {
            CRT_HTTP_CLIENT.execute(nextRequest).get();
        } catch (Exception e) {
            throw new LambdaRuntimeClientException("Failed to get next invoke", e);
        }

        return NEXT_REQUEST_HANDLER.getInvocationRequest();
    }

    public void postInvocationResponse(String requestId, byte[] response) {
        URI endpoint = URI.create(String.format(INVOCATION_SUCCESS_URL_TEMPLATE, hostname, port, requestId));
        HttpRequest invocationResponseRequest = HttpRequest.newBuilder(endpoint)
                .header("User-Agent", USER_AGENT)
                .POST(HttpRequest.BodyPublishers.ofByteArray(response))
                .build();

        try {
            HTTP_CLIENT.send(invocationResponseRequest, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            throw new LambdaRuntimeClientException("Failed to post invocation result", e);
        }
    }

    public void postInvocationError(String requestId, byte[] errorResponse, String errorType) throws IOException {
        postInvocationError(requestId, errorResponse, errorType, null);
    }

    public void postInvocationError(String requestId, byte[] errorResponse, String errorType, String errorCause)
            throws IOException {
        String endpoint = invocationErrorEndpoint(requestId);
        post(endpoint, errorResponse, errorType, errorCause);
    }

    public void getRestoreNext() throws IOException {
        doGet(restoreNextEndpoint(), HTTP_OK);
    }

    public int postRestoreError(byte[] errorResponse, String errorType) throws IOException {
        String endpoint = restoreErrorEndpoint();
        return postError(endpoint, errorResponse, errorType, null);
    }

    public void postInitError(byte[] errorResponse, String errorType) throws IOException {
        String endpoint = initErrorEndpoint();
        post(endpoint, errorResponse, errorType, null);
    }

    private void post(String endpoint, byte[] errorResponse, String errorType, String errorCause) throws IOException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(endpoint))
                .POST(HttpRequest.BodyPublishers.ofByteArray(errorResponse))
                .header("User-Agent", USER_AGENT)
                .header("Content-Type", DEFAULT_CONTENT_TYPE);

        if (errorType != null && !errorType.isEmpty()) {
            request.header(ERROR_TYPE_HEADER, errorType);
        }
        if (errorCause != null && errorCause.getBytes().length < XRAY_ERROR_CAUSE_MAX_HEADER_SIZE) {
            request.header(XRAY_ERROR_CAUSE_HEADER, errorCause);
        }

        HttpResponse<Void> response;
        try {
            response = HTTP_CLIENT.send(request.build(), HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException | IOException e) {
            throw new LambdaRuntimeClientException("Failed to post error", e);
        }

        if (response.statusCode() != HTTP_ACCEPTED) {
            throw new LambdaRuntimeClientException(
                    String.format("%s Response code: '%d'.", endpoint, response.statusCode()));
        }
    }

    private String invocationEndpoint() {
        return getBaseUrl() + "/2018-06-01/runtime/invocation/";
    }

    private String invocationErrorEndpoint(String requestId) {
        return invocationEndpoint + requestId + "/error";
    }

    private String initErrorEndpoint() {
        return getBaseUrl() + "/2018-06-01/runtime/init/error";
    }

    private String restoreErrorEndpoint() {
        return getBaseUrl() + "/2018-06-01/runtime/restore/error";
    }

    private String restoreNextEndpoint() {
        return getBaseUrl() + "/2018-06-01/runtime/restore/next";
    }

    private String getBaseUrl() {
        return "http://" + hostname + ":" + port;
    }

    private int postError(String endpoint,
                          byte[] errorResponse,
                          String errorType,
                          String errorCause) throws IOException {

        Map<String, String> headers = new HashMap<>();
        if (errorType != null && !errorType.isEmpty()) {
            headers.put(ERROR_TYPE_HEADER, errorType);
        }
        if (errorCause != null && errorCause.getBytes().length < XRAY_ERROR_CAUSE_MAX_HEADER_SIZE) {
            headers.put(XRAY_ERROR_CAUSE_HEADER, errorCause);
        }

        return doPost(endpoint, DEFAULT_CONTENT_TYPE, headers, errorResponse);
    }

    private int doPost(String endpoint,
                       String contentType,
                       Map<String, String> headers,
                       byte[] payload) throws IOException {

        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(endpoint))
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                .header("User-Agent", USER_AGENT)
                .header("Content-Type", DEFAULT_CONTENT_TYPE);

        for (Map.Entry<String, String> header : headers.entrySet()) {
            request.header(header.getKey(), header.getValue());
        }

        HttpResponse<Void> response;
        try {
            response = HTTP_CLIENT.send(request.build(), HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException | IOException e) {
            throw new LambdaRuntimeClientException("Failed to post error", e);
        }
    }

    private void doGet(String endpoint, int expectedHttpResponseCode) throws IOException {

        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(endpoint))
                .GET()
                .header("User-Agent", USER_AGENT)
                .header("Content-Type", DEFAULT_CONTENT_TYPE);

        HttpResponse<Void> response;
        try {
            response = HTTP_CLIENT.send(request.build(), HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException | IOException e) {
            throw new LambdaRuntimeClientException("Failed to post error", e);
        }

        if (response.statusCode() != expectedHttpResponseCode) {
            throw new LambdaRuntimeClientException(endpoint, response.statusCode());
        }
    }

    private URL createUrl(String endpoint) {
        /* 
        try {
            response = HTTP_CLIENT.send(nextRequest, HttpResponse.BodyHandlers.ofByteArray());
        } catch (Exception e) {
            throw new LambdaRuntimeClientException("Failed to get next invoke", e);
        }

        return invocationRequestFromHttpResponse(response);
        */
    }
}
