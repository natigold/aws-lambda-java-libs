package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.hamcrest.CoreMatchers;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class LambdaRuntimeClientTest {

    MockWebServer mockWebServer;
    LambdaRuntimeClient runtimeClient;

    @BeforeEach
    void setUp() {
        mockWebServer = new MockWebServer();
        String hostnamePort = getHostnamePort();
        runtimeClient = new LambdaRuntimeClient(hostnamePort);
    }

    @NotNull
    private String getHostnamePort() {
        return mockWebServer.getHostName() + ":" + mockWebServer.getPort();
    }

    @Test
    void postInvocationResponse() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse());
        String requestId = UUID.randomUUID().toString();
        String body = "{}";
        runtimeClient.postInvocationResponse(requestId, body.getBytes());

        RecordedRequest recordedRequest = mockWebServer.takeRequest();
        HttpUrl actualUrl = recordedRequest.getRequestUrl();
        assertNotNull(actualUrl);
        String expectedUrl = "http://" + getHostnamePort() + "/2018-06-01/runtime/invocation/" + requestId + "/response";
        assertEquals(expectedUrl, actualUrl.toString());

        String userAgent = recordedRequest.getHeader("User-Agent");
        assertThat(userAgent, CoreMatchers.startsWith("aws-lambda-java/"));

        String actualBody = recordedRequest.getBody().readUtf8();
        // assertEquals(body, actualBody);
    }

    @Test
    void postInvocationError_noCause() throws IOException, InterruptedException {
        MockResponse mockResponse = new MockResponse();
        mockResponse.setResponseCode(202);
        mockWebServer.enqueue(mockResponse);
        String requestId = UUID.randomUUID().toString();
        String errorResponse = "{}";
        String errorType = "errorType";
        runtimeClient.postInvocationError(requestId, errorResponse.getBytes(), errorType);

        RecordedRequest recordedRequest = mockWebServer.takeRequest();
        HttpUrl actualUrl = recordedRequest.getRequestUrl();
        assertNotNull(actualUrl);
        String expectedUrl = "http://" + getHostnamePort() + "/2018-06-01/runtime/invocation/" + requestId + "/error";
        assertEquals(expectedUrl, actualUrl.toString());

        String userAgent = recordedRequest.getHeader("User-Agent");
        assertThat(userAgent, CoreMatchers.startsWith("aws-lambda-java/"));

        String contentType = recordedRequest.getHeader("Content-Type");
        assertEquals(contentType, "application/json");

        String actualErrorType = recordedRequest.getHeader("Lambda-Runtime-Function-Error-Type");
        assertEquals(errorType, actualErrorType);
    }

    @Test
    void postInvocationError_withCause() throws IOException, InterruptedException {
        MockResponse mockResponse = new MockResponse();
        mockResponse.setResponseCode(202);
        mockWebServer.enqueue(mockResponse);
        String requestId = UUID.randomUUID().toString();
        String errorResponse = "{}";
        String errorType = "errorType";
        String errorCause = "errorCause";
        runtimeClient.postInvocationError(requestId, errorResponse.getBytes(), errorType, errorCause);

        RecordedRequest recordedRequest = mockWebServer.takeRequest();
        HttpUrl actualUrl = recordedRequest.getRequestUrl();
        assertNotNull(actualUrl);
        String expectedUrl = "http://" + getHostnamePort() + "/2018-06-01/runtime/invocation/" + requestId + "/error";
        assertEquals(expectedUrl, actualUrl.toString());

        String userAgent = recordedRequest.getHeader("User-Agent");
        assertThat(userAgent, CoreMatchers.startsWith("aws-lambda-java/"));

        String contentType = recordedRequest.getHeader("Content-Type");
        assertEquals(contentType, "application/json");

        String actualErrorType = recordedRequest.getHeader("Lambda-Runtime-Function-Error-Type");
        assertEquals(errorType, actualErrorType);

        String actualErrorCause = recordedRequest.getHeader("Lambda-Runtime-Function-XRay-Error-Cause");
        assertEquals(errorCause, actualErrorCause);
    }

    @Test
    void postInvocationError_wrongStatusCode() {
        MockResponse mockResponse = new MockResponse();
        mockResponse.setResponseCode(200);
        mockWebServer.enqueue(mockResponse);
        String requestId = UUID.randomUUID().toString();
        String errorResponse = "{}";
        String errorType = "errorType";
        Exception exception = assertThrows(LambdaRuntimeClientException.class, () -> runtimeClient.postInvocationError(requestId, errorResponse.getBytes(), errorType));
        String expectedMessage = "http://" + getHostnamePort() + "/2018-06-01/runtime/invocation/" + requestId + "/error Response code: '200'.";
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    void postInitError() throws IOException, InterruptedException {
        MockResponse mockResponse = new MockResponse();
        mockResponse.setResponseCode(202);
        mockWebServer.enqueue(mockResponse);
        String errorResponse = "{}";
        String errorType = "errorType";
        runtimeClient.postInitError(errorResponse.getBytes(), errorType);

        RecordedRequest recordedRequest = mockWebServer.takeRequest();
        HttpUrl actualUrl = recordedRequest.getRequestUrl();
        assertNotNull(actualUrl);
        String expectedUrl = "http://" + getHostnamePort() + "/2018-06-01/runtime/init/error";
        assertEquals(expectedUrl, actualUrl.toString());

        String userAgent = recordedRequest.getHeader("User-Agent");
        assertThat(userAgent, CoreMatchers.startsWith("aws-lambda-java/"));

        String contentType = recordedRequest.getHeader("Content-Type");
        assertEquals(contentType, "application/json");

        String actualErrorType = recordedRequest.getHeader("Lambda-Runtime-Function-Error-Type");
        assertEquals(errorType, actualErrorType);
    }
}