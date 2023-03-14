package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import reactor.netty.http.client.HttpClientResponse;
import reactor.util.function.Tuple2;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * LambdaRuntimeClient is a client of the AWS Lambda Runtime HTTP API for custom runtimes.
 * <p>
 * API definition can be found at https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html
 * <p>
 * Copyright (c) 2019 Amazon. All rights reserved.
 */
public class LambdaRuntimeClient {

    private static final String INVOCATION_ERROR_URL_TEMPLATE = "http://%s/2018-06-01/runtime/invocation/%s/error";
    private static final String INVOCATION_SUCCESS_URL_TEMPLATE = "http://%s/2018-06-01/runtime/invocation/%s/response";
    private static final String NEXT_URL_TEMPLATE = "http://%s/2018-06-01/runtime/invocation/next";
    private static final String INIT_ERROR_URL_TEMPLATE = "http://%s/2018-06-01/runtime/init/error";

    private static final String DEFAULT_CONTENT_TYPE = "application/json";

    private static final String XRAY_ERROR_CAUSE_HEADER = "Lambda-Runtime-Function-XRay-Error-Cause";
    private static final String ERROR_TYPE_HEADER = "Lambda-Runtime-Function-Error-Type";

    private static final String REQUEST_ID_HEADER = "lambda-runtime-aws-request-id";
    private static final String FUNCTION_ARN_HEADER = "lambda-runtime-invoked-function-arn";
    private static final String DEADLINE_MS_HEADER = "lambda-runtime-deadline-ms";
    private static final String TRACE_ID_HEADER = "lambda-runtime-trace-id";
    private static final String CLIENT_CONTEXT_HEADER = "lambda-runtime-client-context";
    private static final String COGNITO_IDENTITY_HEADER = "lambda-runtime-cognito-identity";

    private static final String USER_AGENT = String.format(
            "aws-lambda-java/%s",
            System.getProperty("java.vendor.version"));

    // private static final OkHttpClient HTTP_CLIENT = new OkHttpClient.Builder()
    //         .readTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
    //         .writeTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
    //         .callTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
    //         .connectTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
    //         .build();

    private static final HttpClient HTTP_CLIENT = HttpClient
            .create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Integer.MAX_VALUE)
            .headers(h -> h.set(HttpHeaderNames.USER_AGENT, USER_AGENT));
    
    private final String hostnamePort;


    public LambdaRuntimeClient(String hostnamePort) {
        Objects.requireNonNull(hostnamePort, "hostnamePort cannot be null");
        this.hostnamePort = hostnamePort;

        HTTP_CLIENT.warmup()
            .block();
    }

    public InvocationRequest waitForNextInvocation() {
        try {
            byte[] empty = {};

            Tuple2<byte[], HttpClientResponse> response = 
                HTTP_CLIENT
                    .get()
                    .uri(String.format(NEXT_URL_TEMPLATE, hostnamePort))
                    .responseSingle((resp, byteBuf) ->
                    (Mono<Tuple2<byte[], HttpClientResponse>>)Mono.zip(byteBuf.asByteArray()
                                    .switchIfEmpty(Mono.just(empty)),
                                    Mono.just(resp)))
                    .doOnError(e -> new LambdaRuntimeClientException("Failed to get next invoke", e))
                    .block();
                    
            return invocationRequestFromHttpResponse(response);
        } catch (IOException e) {
            throw new LambdaRuntimeClientException("Failed to get next invoke", e);
        }
    }

    private InvocationRequest invocationRequestFromHttpResponse(Tuple2<byte[], HttpClientResponse> response) throws IOException {
        InvocationRequest result = new InvocationRequest();

        result.id = response.getT2().responseHeaders().get(REQUEST_ID_HEADER);
        if (result.id == null) {
            throw new LambdaRuntimeClientException("Request ID absent");
        }

        result.invokedFunctionArn = response.getT2().responseHeaders().get(FUNCTION_ARN_HEADER);
        if (result.invokedFunctionArn == null) {
            throw new LambdaRuntimeClientException("Function ARN absent");
        }

        String deadlineMs = response.getT2().responseHeaders().get(DEADLINE_MS_HEADER);
        result.deadlineTimeInMs = deadlineMs == null ? 0 : Long.parseLong(deadlineMs);

        result.xrayTraceId = response.getT2().responseHeaders().get(TRACE_ID_HEADER);
        result.clientContext = response.getT2().responseHeaders().get(CLIENT_CONTEXT_HEADER);
        result.cognitoIdentity = response.getT2().responseHeaders().get(COGNITO_IDENTITY_HEADER);

        result.content = response.getT1();

        return result;
    }

    public void postInvocationSuccess(String requestId, byte[] payload) {
        try {
            HTTP_CLIENT.post()
                .uri(String.format(INVOCATION_SUCCESS_URL_TEMPLATE, hostnamePort, requestId))
                .send(Mono.just(Unpooled.wrappedBuffer(payload)))
                .response()
                .block();
        } catch (RuntimeException e) {
            throw new LambdaRuntimeClientException("Failed to post invocation result", e);
        }
    }

    public void postInvocationError(InvocationError invocationError) {
        String endpoint = String.format(INVOCATION_ERROR_URL_TEMPLATE, hostnamePort, invocationError.getId());
        post(endpoint, invocationError);
    }

    public void postInitError(InvocationError invocationError) {
        String endpoint = String.format(INIT_ERROR_URL_TEMPLATE, hostnamePort);
        post(endpoint, invocationError);
    }

    private void post(String endpoint, InvocationError invocationError) {

        Map<String, String> headers = new HashMap<String, String>();

        headers.put("Content-Type", DEFAULT_CONTENT_TYPE);

        if (invocationError.getErrorType().isPresent()) {
            headers.put(ERROR_TYPE_HEADER, invocationError.getErrorType().get());
        }
        if (invocationError.getErrorCause().isPresent()) {
            headers.put(XRAY_ERROR_CAUSE_HEADER, invocationError.getErrorCause().get());
        }

        HttpClientResponse response =
            HTTP_CLIENT
                .headers(h -> headers.forEach(h::set))
                .post()
                .uri(endpoint)
                .send(Mono.just(Unpooled.wrappedBuffer(invocationError.getErrorResponse())))
                .response()
                .doOnError(e -> new LambdaRuntimeClientException(String.format("%s Response code: FAILED.", endpoint)))
                .block();

        if (Objects.nonNull(response.status()) && response.status().code() != HttpResponseStatus.ACCEPTED.code()) {
            throw new LambdaRuntimeClientException(
                    String.format("%s Response code: '%d'.", endpoint, response.status().code()));
        }
    }
}
