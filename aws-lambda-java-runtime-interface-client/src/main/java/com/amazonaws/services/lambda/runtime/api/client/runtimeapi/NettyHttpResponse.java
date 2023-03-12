package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import reactor.netty.http.client.HttpClientResponse;

public class NettyHttpResponse {
    private HttpClientResponse response;
    private byte[] content;

    public NettyHttpResponse(HttpClientResponse response) {
        this.response = response;
    }

    public NettyHttpResponse(HttpClientResponse response, byte[] content) {
        this.response = response;
        this.content = content;
    }

    public HttpClientResponse getResponse() {
        return response;
    }

    public byte[] getContent() {
        return content;
    }
}
