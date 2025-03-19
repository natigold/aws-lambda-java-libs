/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/
package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import com.amazonaws.services.lambda.runtime.api.client.runtimeapi.dto.InvocationRequest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;

/**
 * This module defines the native Runtime Interface Client which is responsible for HTTP
 * interactions with the Runtime API.
 */
class NativeClient {

    private static final String NEXT_URL_TEMPLATE = "http://%s/2018-06-01/runtime/invocation/next";
    private static final String INVOCATION_SUCCESS_URL_TEMPLATE = "http://%s/2018-06-01/runtime/invocation/%s/response";

    private static final String REQUEST_ID_HEADER = "lambda-runtime-aws-request-id";
    private static final String FUNCTION_ARN_HEADER = "lambda-runtime-invoked-function-arn";
    private static final String DEADLINE_MS_HEADER = "lambda-runtime-deadline-ms";
    private static final String TRACE_ID_HEADER = "lambda-runtime-trace-id";
    private static final String CLIENT_CONTEXT_HEADER = "lambda-runtime-client-context";
    private static final String COGNITO_IDENTITY_HEADER = "lambda-runtime-cognito-identity";

    private static String hostnamePort;
    private static HttpRequest nextRequest;

    private static final String USER_AGENT;
    private static final HttpClient HTTP_CLIENT;

    static {
        USER_AGENT = String.format(
            "aws-lambda-java/%s",
            System.getProperty("java.vendor.version"));
        HTTP_CLIENT = 
            HttpClient.newBuilder().
                       version(HttpClient.Version.HTTP_1_1).
                       followRedirects(HttpClient.Redirect.NEVER).
                       connectTimeout(Duration.ofSeconds(5)).
                       executor(Executors.newFixedThreadPool(10)).
                       build();
    }

    static void init(String awsLambdaRuntimeApi) {
        Objects.requireNonNull(awsLambdaRuntimeApi, "hostnamePort cannot be null");
        hostnamePort = awsLambdaRuntimeApi;
        nextRequest = 
            HttpRequest.newBuilder(URI.create(String.format(NEXT_URL_TEMPLATE, hostnamePort))).header("User-Agent", USER_AGENT).GET().build();
    }
    
    static InvocationRequest next() {
        HttpResponse<byte[]> response;
        try {
            response = HTTP_CLIENT.send(nextRequest, HttpResponse.BodyHandlers.ofByteArray());
        } catch (Exception e) {
            throw new LambdaRuntimeClientException("Failed to get next invoke", 500);
        }

        return invocationRequestFromHttpResponse(response);
    }

    static void postInvocationResponse(String requestId, byte[] response) {
        URI endpoint = URI.create(String.format(INVOCATION_SUCCESS_URL_TEMPLATE, hostnamePort, requestId));
        HttpRequest invocationResponseRequest = 
            HttpRequest.newBuilder(endpoint).header("User-Agent", USER_AGENT).POST(HttpRequest.BodyPublishers.ofByteArray(response)).build();

        try {
            HTTP_CLIENT.send(invocationResponseRequest, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            throw new LambdaRuntimeClientException("Failed to post invocation result", 500);
        }

    }

    private static InvocationRequest invocationRequestFromHttpResponse(HttpResponse<byte[]> response) {
        InvocationRequest result = new InvocationRequest();

        result.setId(response.headers().firstValue(REQUEST_ID_HEADER).orElseThrow(
                () -> new LambdaRuntimeClientException("Request ID absent", 500)));
        result.setInvokedFunctionArn(response.headers().firstValue(FUNCTION_ARN_HEADER).orElseThrow(
                () -> new LambdaRuntimeClientException("Function ARN absent", 500)));
        result.setDeadlineTimeInMs(Long.parseLong(response.headers().firstValue(DEADLINE_MS_HEADER).orElse("0")));
        result.setXrayTraceId(response.headers().firstValue(TRACE_ID_HEADER).orElse(null));
        result.setClientContext(response.headers().firstValue(CLIENT_CONTEXT_HEADER).orElse(null));
        result.setCognitoIdentity(response.headers().firstValue(COGNITO_IDENTITY_HEADER).orElse(null));
        result.setContent(response.body());

        return result;
    }

}
