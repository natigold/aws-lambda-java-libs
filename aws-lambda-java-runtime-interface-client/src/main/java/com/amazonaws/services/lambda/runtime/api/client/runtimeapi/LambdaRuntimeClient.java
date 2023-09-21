package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.swing.text.Style;

import org.json.JSONException;
import org.json.JSONObject;

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

    private final URI uri;

    public LambdaRuntimeClient(String hostnamePort) {
        Objects.requireNonNull(hostnamePort, "hostnamePort cannot be null");
        String[] parts = hostnamePort.split(":");
        this.hostname = parts[0];
        this.port = Integer.parseInt(parts[1]);
        this.invocationEndpoint = invocationEndpoint();

        try {
            this.uri = new URI(getBaseUrl());
        } catch (Exception e) {
            throw new LambdaRuntimeClientException("Failed to initialize connection", e);
        }
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
    
    public HttpResponse doRequest(String method, String path, byte[] requestBody) {
        HttpResponse response = null;
        String dataStr = "";

        Map<String, String> headers = new HashMap<String, String> ( 
            Map.of(
                "Host", String.format("%s:%s", hostname, port),
                "Content-Type", "application/json"
            )
        );

        if (Objects.nonNull(requestBody) && requestBody.length > 0) {
            // @@TODO: Fix data format to not encoded version and un-remark the Test assertion comment
            dataStr += String.format(" -d %s", URLEncoder.encode(new String(requestBody, StandardCharsets.UTF_8), StandardCharsets.UTF_8));
        }

        StringBuilder headersStr = new StringBuilder("");
        headers.entrySet().forEach(header -> {
            headersStr.append(String.format(" -H '%s: %s'", header.getKey(), header.getValue()));
        });

        String command = String.format("curl -v --silent -A %s -X %s%s %s%s %s", USER_AGENT, method, headersStr, this.uri, path, dataStr);

        ProcessBuilder pb = new ProcessBuilder(command.split(" "));
        // errorstream of the process will be redirected to standard output
        pb.redirectErrorStream(true);
        
        Process p = null;

        try {
            p = pb.start();

            InputStream is = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);

            String result = new BufferedReader(new InputStreamReader(is))
                    .lines()
                    .parallel()
                    .collect(Collectors.joining("\n"));
    
            // close the buffered reader
            br.close();
            p.waitFor();
            response = resolve(result);
            
            int exitCode = p.exitValue();

            p.destroy();

            return response;
        } catch (IOException | InterruptedException e) {
            throw new LambdaRuntimeClientException("Failed to issue cURL request", e);
        }
    }

    public InvocationRequest getInvocationRequest(HttpResponse response) {
        InvocationRequest request = new InvocationRequest();

        if (response.content != null && response.content.length > 0) {
            request.setContent(response.content);
        } else {
            String body = new String("{}");
            request.setContent(body.getBytes(StandardCharsets.UTF_8));
        }

        request.id = null;
        request.invokedFunctionArn = null;
        request.deadlineTimeInMs = 0;
        request.xrayTraceId = null;
        request.clientContext = null;
        request.cognitoIdentity = null;

        for (Map.Entry<String,String[]> header : response.headers.entrySet()) {
            switch (header.getKey()) {
                case REQUEST_ID_HEADER:
                    request.id = header.getValue()[0];
                    break;
            
                case FUNCTION_ARN_HEADER:
                    request.invokedFunctionArn = header.getValue()[0];
                    break;
            
                case DEADLINE_MS_HEADER:
                    request.deadlineTimeInMs = Long.parseLong(header.getValue()[0]);
                    break;
            
                case TRACE_ID_HEADER:
                    request.xrayTraceId = header.getValue()[0];
                    break;
            
                case CLIENT_CONTEXT_HEADER:
                    request.clientContext = header.getValue()[0];
                    break;
            
                case COGNITO_IDENTITY_HEADER:
                    request.cognitoIdentity = header.getValue()[0];
                    break;
            
                default:
                    break;
            }
        }

        Optional.ofNullable(request.id)
                .orElseThrow(() -> new LambdaRuntimeClientException("Request ID absent"));
        Optional.ofNullable(request.invokedFunctionArn)
                .orElseThrow(() -> new LambdaRuntimeClientException("Function ARN absent"));

        return request;
    }

    private HttpResponse resolve(String response) {
		Stream<String> stream = response.lines();
        HttpResponse httpResponse = new HttpResponse();

        for (String line : stream.collect(Collectors.toList())) {
            parseLine(line, httpResponse);
        }

        return httpResponse;
    }

    HttpResponse parseLine(String line, HttpResponse httpResponse) {        
        String regex = "^(\\*|<|>|\\{)+ (.*)?";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);

        while (matcher.find()) {
            // Response
            if (matcher.group(1).startsWith("<")) {
                if (matcher.group(2).startsWith("HTTP/1.1")) {
                    Matcher responseCodeMatcher = Pattern.compile("HTTP/1.1 (\\d+)").matcher(matcher.group(2));
                    if (responseCodeMatcher.find()) {
                        httpResponse.responseCode = Integer.parseInt(responseCodeMatcher.group(1));
                    }
                } else {
                    Matcher headerMatcher = Pattern.compile("(.*): (.*)").matcher(matcher.group(2));
                    if (headerMatcher.find()) {
                        httpResponse.headers.put(headerMatcher.group(1), 
                            new String[] {headerMatcher.group(2)});
                    }
                }
            } else if (matcher.group(1).startsWith("*")) {
                // Comment line, skipping
            } else if (matcher.group(1).startsWith("{")) {
                try {
                    JSONObject jsonObject = new JSONObject(line);
                    httpResponse.content = line.getBytes();
                } catch (JSONException e) {
                    // Non-content line, skipping
                } catch (Exception e) {
                    // Illformated JSON line, skipping
                }
            }
        }
        
        return httpResponse;
    }

    class HttpResponse {
        Map<String, String[]> headers = new HashMap<>();
        byte[] content = null;
        int responseCode;
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("HttpResponse [");
            sb.append(String.format("responseCode=%d, ", responseCode));
            headers.forEach((k, v) -> {
                sb.append(String.format("header %s=%s, ", k, v[0]));
            });
            if (content != null && content.length > 0) {
                sb.append(String.format("content=%s, ", new String(content)));
            }
            sb.append("]");
            return sb.toString();
        }    
    }
}