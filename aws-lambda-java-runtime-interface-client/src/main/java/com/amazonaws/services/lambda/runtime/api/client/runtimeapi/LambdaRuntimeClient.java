package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.CrtResource;
import software.amazon.awssdk.crt.http.HttpClientConnection;
import software.amazon.awssdk.crt.http.HttpClientConnectionManager;
import software.amazon.awssdk.crt.http.HttpClientConnectionManagerOptions;
import software.amazon.awssdk.crt.http.HttpRequest;
import software.amazon.awssdk.crt.io.ClientBootstrap;
import software.amazon.awssdk.crt.io.EventLoopGroup;
import software.amazon.awssdk.crt.io.HostResolver;
import software.amazon.awssdk.crt.io.SocketOptions;
import software.amazon.awssdk.crt.http.HttpHeader;
import software.amazon.awssdk.crt.http.HttpRequestBase;
import software.amazon.awssdk.crt.http.HttpStreamBase;
import software.amazon.awssdk.crt.http.HttpRequestBodyStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static software.amazon.awssdk.crt.utils.ByteBufferUtils.transferData;

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

    private static final String USER_AGENT = String.format(
            "aws-lambda-java/%s",
            System.getProperty("java.vendor.version"));

    private HttpClientConnectionManager connPool = null;
    private HttpClientConnection conn = null;

    private CompletableFuture<Void> shutdownComplete = null;
    private boolean actuallyConnected = false;

    public LambdaRuntimeClient(String hostnamePort) {
        Objects.requireNonNull(hostnamePort, "hostnamePort cannot be null");
        String[] parts = hostnamePort.split(":");
        this.hostname = parts[0];
        this.port = Integer.parseInt(parts[1]);
        this.invocationEndpoint = invocationEndpoint();

        try {
            URI uri = new URI(getBaseUrl());
            this.connPool = createConnectionPoolManager(uri);
            this.shutdownComplete = connPool.getShutdownCompleteFuture();

            this.conn = connPool.acquireConnection().get(60, TimeUnit.SECONDS);
            this.actuallyConnected = true;
        } catch (Exception e) {
            throw new LambdaRuntimeClientException("Failed to initialize connection", e);
        }
    }

    private HttpClientConnectionManager createConnectionPoolManager(URI uri) {
        try (EventLoopGroup eventLoopGroup = new EventLoopGroup(1);
                HostResolver resolver = new HostResolver(eventLoopGroup);
                ClientBootstrap bootstrap = new ClientBootstrap(eventLoopGroup, resolver);
                SocketOptions sockOpts = new SocketOptions();) {

            HttpClientConnectionManagerOptions options = new HttpClientConnectionManagerOptions()
                    .withClientBootstrap(bootstrap)
                    .withSocketOptions(sockOpts)
                    .withUri(uri);

            return HttpClientConnectionManager.create(options);
        }
    }

    public InvocationRequest waitForNextInvocation() {
        return doRequest("GET", getBaseUrl(), NEXT_INVOCATION_PATH, new byte[0], false, 200)
                .getInvocationRequest();
    }

    public void postInvocationResponse(String requestId, byte[] response) {
        String path = String.format(INVOCATION_SUCCESS_PATH_TEMPLATE, requestId);

        try {
            doRequest("POST", getBaseUrl(), path, response, false, HTTP_OK);
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
    
    public CrtStreamResponseHandler doRequest(String method, String endpoint, String path, byte[] requestBody,
            boolean useChunkedEncoding, int expectedStatus) {
        URI uri = null;

        try {
            uri = new URI(endpoint);
        } catch (URISyntaxException e) {
            throw new LambdaRuntimeClientException("Error forming URI", e);
        }

        HttpHeader[] requestHeaders = new HttpHeader[] { 
            new HttpHeader("Host", uri.getHost()),
            new HttpHeader("Content-Length", Integer.toString(requestBody.length)),
            new HttpHeader("User-Agent", USER_AGENT) 
        };
        
        final ByteBuffer bodyBytesIn = ByteBuffer.wrap(requestBody);
   
        HttpRequestBodyStream bodyStream = new HttpRequestBodyStream() {
            @Override
            public boolean sendRequestBody(ByteBuffer bodyBytesOut) {
                transferData(bodyBytesIn, bodyBytesOut);

                return bodyBytesIn.remaining() == 0;
            }

            @Override
            public boolean resetPosition() {
                bodyBytesIn.position(0);

                return true;
            }
        };

        HttpRequest request = new HttpRequest(method, path, requestHeaders, bodyStream);

        if (request.getBodyStream() != null) {
            request.getBodyStream().resetPosition();
        }

        CrtStreamResponseHandler response = null;
        try {
            response = getResponse(uri, request);
        } catch (Exception ex) {
            throw new LambdaRuntimeClientException("Failed to get response from request", ex);
        }

        return response;
    }

    public CrtStreamResponseHandler getResponse(URI uri, HttpRequestBase request) throws Exception {
        CompletableFuture<Void> requestCompletableFuture = new CompletableFuture<>();
        CrtStreamResponseHandler responseHandler = new CrtStreamResponseHandler(requestCompletableFuture);

        if (!actuallyConnected) {
            this.conn = connPool.acquireConnection().get(60, TimeUnit.SECONDS);
            actuallyConnected = true;
        }

        HttpStreamBase stream = conn.makeRequest(request, responseHandler);
        stream.activate();
        requestCompletableFuture.get(60, TimeUnit.SECONDS);
        stream.close();

        return responseHandler;
    }
}
