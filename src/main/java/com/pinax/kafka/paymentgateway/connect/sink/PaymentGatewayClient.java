package com.pinax.kafka.paymentgateway.connect.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;

public class PaymentGatewayClient {
    private final Logger logger = LoggerFactory.getLogger(PaymentGatewaySinkConnector.class);

    private final ManagedChannel channel;
    private final CallCredentials callCredentials;

    public PaymentGatewayClient(String endpoint, String token) {
        // Sets the token to be used for authentication
        this.callCredentials = new BearerToken(token);

        // TODO: Initialize the gRPC channel with the credentials added

        // Create the report stub
        this.channel = null;
    }

    public void start() {
        logger.info("Starting PaymentGateway client");
    }

    public void stop() {
        logger.info("Stopping PaymentGateway client");
    }

    public void report() {

    }
}
