package com.pinax.kafka.paymentgateway.connect.sink;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.util.JsonFormat;

import org.apache.kafka.connect.sink.SinkRecord;

import io.grpc.CallCredentials;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ManagedChannel;

import sf.gateway.payment.v1.*;
import sf.gateway.payment.v1.Gateway.ReportRequest;
import sf.metering.v1.MeteringOuterClass.Event;

public class PaymentGatewayClient {
    private final Logger logger = LoggerFactory.getLogger(PaymentGatewaySinkConnector.class);

    private final String endpoint;
    private final CallCredentials callCredentials;

    private ManagedChannel channel;
    private UsageServiceGrpc.UsageServiceBlockingStub blockingStub;

    public PaymentGatewayClient(String endpoint, String token) {
        this.endpoint = endpoint;

        // Sets the token to be used for authentication
        this.callCredentials = new BearerToken(token);

        logger.info("ENDPOINT: {}", this.endpoint);
    }

    public void start() {
        logger.info("Starting PaymentGateway client");

        // Create the channel
        URI uri = URI.create(this.endpoint);
        String host = uri.getHost();
        int port = uri.getPort();

        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        // Create the blocking stub
        this.blockingStub = UsageServiceGrpc.newBlockingStub(channel)
                .withCallCredentials(callCredentials);

        logger.info("Started PaymentGateway client");
    }

    public void stop() {
        logger.info("Stopping PaymentGateway client");
        if (channel != null) {
            channel.shutdown();
        }
        logger.info("Stopped PaymentGateway client");
    }

    public void report(Collection<SinkRecord> records) {
        logger.info("Reporting usage to PaymentGateway");

        try {
            List<Event> events = new ArrayList<Event>();

            // int i = 0; // TODO: use batch size
            for (SinkRecord record : records) {
                logger.info("Processing record {}", record);

                // 1. Extract the data from the SinkRecord into a metering event
                Event.Builder eventBuilder = Event.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(record.value().toString(), eventBuilder);
                Event event = eventBuilder.build();

                // 2. Aggregate the metering events
                events.add(event);
                // ++i;
            }

            // 3. Create the request to report the metering event
            ReportRequest reportRequest = ReportRequest.newBuilder()
                    .addAllEvents(events)
                    .build();

            // 4. Send the request to the PaymentGateway
            blockingStub.report(reportRequest);

            logger.info("Reported usage to PaymentGateway");
        } catch (Exception e) {
            logger.error("Failed to report usage to PaymentGateway", e);
        }
    }
}
