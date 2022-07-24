package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import org.reactivestreams.Subscriber;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;

import java.nio.ByteBuffer;
import java.util.Optional;

class EmptyContentPublisher implements SdkHttpContentPublisher {
    @Override
    public Optional<Long> contentLength() {
        return Optional.of(0L);
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s) {

    }
}
