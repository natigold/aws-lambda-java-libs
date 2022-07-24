package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

class BodyByteBufferSubscriber implements Subscriber<ByteBuffer> {
    CompletableFuture<byte[]> completableFuture = new CompletableFuture<>();
    ByteBuffer payload = ByteBuffer.allocate(6 * 1024 * 1024);

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(ByteBuffer byteBuffer) {
        payload.put(byteBuffer);
    }

    @Override
    public void onError(Throwable throwable) {
        completableFuture.completeExceptionally(throwable);
        payload.rewind();
    }

    @Override
    public void onComplete() {
        byte[] bytes = new byte[payload.position()];
        payload.rewind();
        payload.get(bytes);
        payload.rewind();
        completableFuture.complete(bytes);
    }

    byte[] getPaload() {
        try {
            return completableFuture.get();
        } catch (Exception e) {
            throw new LambdaRuntimeClientException("Failed to get payload", e);
        }
    }
}
