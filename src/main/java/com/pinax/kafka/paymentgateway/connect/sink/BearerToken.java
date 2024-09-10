package com.pinax.kafka.paymentgateway.connect.sink;

import java.util.concurrent.Executor;

import io.grpc.CallCredentials;
import io.grpc.Metadata;

public class BearerToken extends CallCredentials {
    private final String token;

    public BearerToken(String token) {
        this.token = token;
    }

    @Override
    public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        applier.apply(headers);
    }

    @Override
    public void thisUsesUnstableApi() {
    }
}
