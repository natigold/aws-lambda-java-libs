package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.hamcrest.CoreMatchers;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.matchers.JUnitMatchers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class LambdaRuntimeClientWaitForNextInvocationTest {
    MockWebServer mockWebServer;
    LambdaRuntimeClient runtimeClient;

    String requestId;
    final String functionArn = "arn:aws:lambda:us-east-1:010203040506:function:java-ric-test";
    final String body = "{ \"foo\": \"bar\" }";
    final long defaultDeadlineTime = 0L;

    MockResponse response = new MockResponse();

    @BeforeEach
    void setUp() {
        mockWebServer = new MockWebServer();
        String hostnamePort = getHostnamePort();
        runtimeClient = new LambdaRuntimeClient(hostnamePort);

        requestId = UUID.randomUUID().toString();

        response.addHeader("lambda-runtime-aws-request-id", requestId);
        response.addHeader("lambda-runtime-invoked-function-arn", functionArn);
        response.setBody(body);
    }

    @NotNull
    private String getHostnamePort() {
        return mockWebServer.getHostName() + ":" + mockWebServer.getPort();
    }

    @Test
    void waitForNextInvocation_basic() throws InterruptedException, IOException {
        mockWebServer.enqueue(response);

        InvocationRequest result = runtimeClient.waitForNextInvocation();
        assertEquals(requestId, result.getId());
        assertEquals(functionArn, result.getInvokedFunctionArn());
        assertEquals(defaultDeadlineTime, result.getDeadlineTimeInMs());
        assertNull(result.getXrayTraceId());
        assertNull(result.getClientContext());
        assertNull(result.getCognitoIdentity());
        assertEquals(body, new String(result.getContentAsStream().readAllBytes()));

        RecordedRequest recordedRequest = mockWebServer.takeRequest();

        HttpUrl actualUrl = recordedRequest.getRequestUrl();
        assertNotNull(actualUrl);
        String expectedUrl = "http://" + getHostnamePort() + "/2018-06-01/runtime/invocation/next";
        assertEquals(expectedUrl, actualUrl.toString());

        String userAgent = recordedRequest.getHeader("User-Agent");
        assertThat(userAgent, CoreMatchers.startsWith("aws-lambda-java/"));
    }

    @Test
    void waitForNextInvocation_delay() {
        int runtimeDeadlineMs = 5000;
        response.addHeader("lambda-runtime-deadline-ms", runtimeDeadlineMs);
        mockWebServer.enqueue(response);

        InvocationRequest result = runtimeClient.waitForNextInvocation();
        assertEquals(runtimeDeadlineMs, result.getDeadlineTimeInMs());
    }

    @Test
    void waitForNextInvocation_traceId() {
        String traceId = UUID.randomUUID().toString();
        response.addHeader("lambda-runtime-trace-id", traceId);
        mockWebServer.enqueue(response);

        InvocationRequest result = runtimeClient.waitForNextInvocation();
        assertEquals(traceId, result.getXrayTraceId());
    }

    @Test
    void waitForNextInvocation_clientContext() {
        String clientContext = UUID.randomUUID().toString();
        response.addHeader("lambda-runtime-client-context", clientContext);
        mockWebServer.enqueue(response);

        InvocationRequest result = runtimeClient.waitForNextInvocation();
        assertEquals(clientContext, result.getClientContext());
    }

    @Test
    void waitForNextInvocation_cognito() {
        String cognitoIdentity = UUID.randomUUID().toString();
        response.addHeader("lambda-runtime-cognito-identity", cognitoIdentity);
        mockWebServer.enqueue(response);

        InvocationRequest result = runtimeClient.waitForNextInvocation();
        assertEquals(cognitoIdentity, result.getCognitoIdentity());
    }

    @Test
    void waitForNextInvocation_functionArnAbsent() {
        MockResponse invalidResponse = new MockResponse();
        invalidResponse.addHeader("lambda-runtime-aws-request-id", requestId);
        mockWebServer.enqueue(invalidResponse);

        Exception exception = assertThrows(LambdaRuntimeClientException.class, () -> runtimeClient.waitForNextInvocation());
        assertEquals("Function ARN absent", exception.getMessage());
    }

    @Test
    void waitForNextInvocation_requestIdAbsent() {
        MockResponse invalidResponse = new MockResponse();
        invalidResponse.addHeader("lambda-runtime-invoked-function-arn", functionArn);
        mockWebServer.enqueue(invalidResponse);

        Exception exception = assertThrows(LambdaRuntimeClientException.class, () -> runtimeClient.waitForNextInvocation());
        assertEquals("Request ID absent", exception.getMessage());
    }
}
