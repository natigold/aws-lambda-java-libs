package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.networknt.client.Http2Client;
import static com.networknt.client.Http2Client.*;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_OK;

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

    private static final String INVOCATION_SUCCESS_PATH_TEMPLATE = "/2018-06-01/runtime/invocation/%s/response";
    private static final String NEXT_INVOCATION_PATH = "/2018-06-01/runtime/invocation/next";

    private static final String DEFAULT_CONTENT_TYPE = "application/json";
    private static final String XRAY_ERROR_CAUSE_HEADER = "Lambda-Runtime-Function-XRay-Error-Cause";
    private static final String ERROR_TYPE_HEADER = "Lambda-Runtime-Function-Error-Type";
    private static final int XRAY_ERROR_CAUSE_MAX_HEADER_SIZE = 1024 * 1024; // 1MiB

    private static final String REQUEST_ID_HEADER = "Lambda-Runtime-Aws-Request-Id";
    private static final String FUNCTION_ARN_HEADER = "Lambda-Runtime-Invoked-Function-Arn";
    private static final String DEADLINE_MS_HEADER = "Lambda-Runtime-Deadline-Ms";
    private static final String TRACE_ID_HEADER = "Lambda-Runtime-Trace-Id";
    private static final String CLIENT_CONTEXT_HEADER = "Lambda-Runtime-Client-Context";
    private static final String COGNITO_IDENTITY_HEADER = "Lambda-Runtime-Cognito-Identity";

    private static final String USER_AGENT = String.format(
            "aws-lambda-java/%s",
            System.getProperty("java.vendor.version"));

    private final ClientConnection conn;
    private final Http2Client client = Http2Client.getInstance();

    public LambdaRuntimeClient(String hostnamePort) {
        Objects.requireNonNull(hostnamePort, "hostnamePort cannot be null");
        String[] parts = hostnamePort.split(":");
        this.hostname = parts[0];
        this.port = Integer.parseInt(parts[1]);
        this.invocationEndpoint = invocationEndpoint();

        try {
            URI uri = new URI(getBaseUrl());
            this.conn = createConnection(uri);
        } catch (Exception e) {
            throw new LambdaRuntimeClientException("Failed to initialize connection", e);
        }
    }

    private ClientConnection createConnection(URI uri) throws Exception {
        CompletableFuture<ClientConnection> clienFuture = this.client.connectAsync(uri, false);
        return clienFuture.get(1000, TimeUnit.MILLISECONDS);
    }

    public InvocationRequest waitForNextInvocation() {
        return getInvocationRequest(doRequest("GET", NEXT_INVOCATION_PATH, new byte[0]));
    }

    public void postInvocationResponse(String requestId, byte[] response) {
        String path = String.format(INVOCATION_SUCCESS_PATH_TEMPLATE, requestId);

        try {
            doRequest("POST", path, response);
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
        URL url = createUrl(endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("User-Agent", USER_AGENT);
        conn.setRequestProperty("Content-Type", DEFAULT_CONTENT_TYPE);
        if (errorType != null && !errorType.isEmpty()) {
            conn.setRequestProperty(ERROR_TYPE_HEADER, errorType);
        }
        if (errorCause != null && errorCause.getBytes().length < XRAY_ERROR_CAUSE_MAX_HEADER_SIZE) {
            conn.setRequestProperty(XRAY_ERROR_CAUSE_HEADER, errorCause);
        }
        conn.setFixedLengthStreamingMode(errorResponse.length);
        conn.setDoOutput(true);
        try (OutputStream outputStream = conn.getOutputStream()) {
            outputStream.write(errorResponse);
        }

        int responseCode = conn.getResponseCode();
        if (responseCode != HTTP_ACCEPTED) {
            throw new LambdaRuntimeClientException(endpoint, responseCode);
        }

        // don't need to read the response, close stream to ensure connection re-use
        closeQuietly(conn.getInputStream());
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

        URL url = createUrl(endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", contentType);

        for (Map.Entry<String, String> header : headers.entrySet()) {
            conn.setRequestProperty(header.getKey(), header.getValue());
        }

        conn.setFixedLengthStreamingMode(payload.length);
        conn.setDoOutput(true);

        try (OutputStream outputStream = conn.getOutputStream()) {
            outputStream.write(payload);
        }

        // get response code before closing the stream
        int responseCode = conn.getResponseCode();

        // don't need to read the response, close stream to ensure connection re-use
        closeInputStreamQuietly(conn);

        return responseCode;
    }

    private void doGet(String endpoint, int expectedHttpResponseCode) throws IOException {

        URL url = createUrl(endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        int responseCode = conn.getResponseCode();
        if (responseCode != expectedHttpResponseCode) {
            throw new LambdaRuntimeClientException(endpoint, responseCode);
        }

        closeInputStreamQuietly(conn);
    }

    private URL createUrl(String endpoint) {
        try {
            return new URL(endpoint);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private void closeQuietly(InputStream inputStream) {
        if (inputStream == null) return;
        try {
            inputStream.close();
        } catch (IOException e) {
        }
    }

    private void closeInputStreamQuietly(HttpURLConnection conn) {

        InputStream inputStream;
        try {
            inputStream = conn.getInputStream();
        } catch (IOException e) {
            return;
        }

        if (inputStream == null) {
            return;
        }
        try {
            inputStream.close();
        } catch (IOException e) {
            // ignore
        }
    }
    
    public ClientResponse doRequest(String method, String path, byte[] requestBody) {
        final AtomicReference<ClientResponse> reference = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        try {
            this.conn.getIoThread().execute(new Runnable() {
                @Override
                public void run() {
                    final ClientRequest request = new ClientRequest().setMethod(new HttpString(method)).setPath(path);

                    request.getRequestHeaders().put(Headers.HOST, getBaseUrl());
                    request.getRequestHeaders().put(Headers.USER_AGENT, USER_AGENT);

                    if (Objects.nonNull(requestBody) && requestBody.length > 0) {
                        request.getRequestHeaders().put(Headers.CONTENT_LENGTH, Integer.toString(requestBody.length));

                    }

                    conn.sendRequest(request, client.createClientCallback(reference, latch, new String(requestBody)));
                }
            });
            latch.await(10, TimeUnit.SECONDS);
            return reference.get();
        } catch (InterruptedException e) {
            throw new LambdaRuntimeClientException("Failed to make request", e);
        }
    }

    public InvocationRequest getInvocationRequest(ClientResponse response) {
        InvocationRequest request = new InvocationRequest();

        request.setContent(response.getAttachment(Http2Client.RESPONSE_BODY).getBytes());

        request.id = null;
        request.invokedFunctionArn = null;
        request.deadlineTimeInMs = 0;
        request.xrayTraceId = null;
        request.clientContext = null;
        request.cognitoIdentity = null;

        response.getResponseHeaders().forEach(header -> {
            switch (header.getHeaderName().toString()) {
                case REQUEST_ID_HEADER:
                    request.id = header.getFirst();
                    break;
            
                case FUNCTION_ARN_HEADER:
                    request.invokedFunctionArn = header.getFirst();
                    break;
            
                case DEADLINE_MS_HEADER:
                    request.deadlineTimeInMs = Long.parseLong(header.getFirst());
                    break;
            
                case TRACE_ID_HEADER:
                    request.xrayTraceId = header.getFirst();
                    break;
            
                case CLIENT_CONTEXT_HEADER:
                    request.clientContext = header.getFirst();
                    break;
            
                case COGNITO_IDENTITY_HEADER:
                    request.cognitoIdentity = header.getFirst();
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
}