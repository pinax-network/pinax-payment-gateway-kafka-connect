package com.pinax.kafka.paymentgateway.connect.sink;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.grpc.CallCredentials;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.Status;
import javax.net.ssl.SSLException;

import sf.gateway.payment.v1.UsageServiceGrpc;
import sf.gateway.payment.v1.UsageServiceGrpc.UsageServiceBlockingStub;
import sf.gateway.payment.v1.Gateway.ReportRequest;
import sf.metering.v1.MeteringOuterClass.Event;

public class PaymentGatewayClient {
    private final Logger logger = LoggerFactory.getLogger(PaymentGatewaySinkConnector.class);

    private final String host;
    private final int port;

    private final int batchSize;

    private final CallCredentials callCredentials;

    public ManagedChannel channel;
    public UsageServiceBlockingStub blockingStub;

    public PaymentGatewayClient(String endpoint, String token, int batchSize) {
        // Should be in the format "http://host:port" since checked in config
        URI uri = URI.create(endpoint);
        this.host = uri.getHost();
        this.port = uri.getPort();

        // Sets the batch size for reporting usage
        this.batchSize = batchSize;

        // Sets the token to be used for authentication
        this.callCredentials = new BearerToken(token);
    }

    private ManagedChannel createChannel(String host, int port) {
        ManagedChannel channel = null;
        try {
            channel = NettyChannelBuilder.forAddress(host, port)
                    .sslContext(GrpcSslContexts.forClient().build())
                    .build();
        } catch (SSLException e) {
            logger.error("Failed to create SSL context", e);
        } catch (Exception e) {
            logger.error("Failed to create channel", e);
        }
        return channel;
    }

    private UsageServiceBlockingStub createBlockingStub(ManagedChannel channel,
            CallCredentials callCredentials) {
        UsageServiceBlockingStub blockingStub = null;
        try {
            blockingStub = UsageServiceGrpc.newBlockingStub(channel)
                    .withCallCredentials(callCredentials);
        } catch (Exception e) {
            logger.error("Failed to create blocking stub", e);
        }
        return blockingStub;
    }

    public void start() {
        try {
            // Create the channel
            this.channel = createChannel(host, port);

            // Create the blocking stub
            this.blockingStub = createBlockingStub(channel, callCredentials);
            logger.info("Started PaymentGateway client");
        } catch (Exception e) {
            logger.error("Failed to start PaymentGateway client", e);
        }
    }

    public void stop() {
        if (this.channel != null) {
            this.channel.shutdown();
        }
        if (this.blockingStub != null) {
            this.blockingStub = null;
        }
        logger.info("Stopped PaymentGateway client");
    }

    public void report(Collection<SinkRecord> records) {
        try {
            List<Event> events = new ArrayList<Event>();

            for (SinkRecord record : records) {
                // 1. Extract the data from the SinkRecord into a metering event
                Event.Builder eventBuilder = Event.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(record.value().toString(), eventBuilder);
                Event event = eventBuilder.build();

                // 2. Aggregate the metering events
                events.add(event);

                if (events.size() >= this.batchSize) {
                    // 3. Create the request to report the metering events
                    ReportRequest reportRequest = ReportRequest.newBuilder()
                            .addAllEvents(events)
                            .build();

                    // 4. Send the request to the PaymentGateway
                    logger.info("events: {} batchSize: {}", events.size(), this.batchSize);
                    this.blockingStub.report(reportRequest);

                    // 5. Reset the list of events
                    events.clear();
                }
            }

            // 6. Create the remaining metering events report request
            ReportRequest reportRequest = ReportRequest.newBuilder()
                    .addAllEvents(events)
                    .build();

            // 7. Send the remaining metering events report request
            if (events.size() > 0) {
                logger.info("events: {} batchSize: {}", events.size(), this.batchSize);
                this.blockingStub.report(reportRequest);
            }

        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to parse protocol buffer", e);
            throw new DataException("Failed to parse protocol buffer", e);
        } catch (StatusRuntimeException e) {
            Status.Code code = e.getStatus().getCode();
            if (code == Status.Code.UNAUTHENTICATED ||
                    code == Status.Code.UNAVAILABLE ||
                    code == Status.Code.PERMISSION_DENIED ||
                    code == Status.Code.RESOURCE_EXHAUSTED ||
                    code == Status.Code.INTERNAL ||
                    code == Status.Code.UNKNOWN) {
                logger.error("gRPC error: {}", e.getStatus(), e);
                throw new RetriableException("Failed to report usage to PaymentGateway due to gRPC error", e);
            } else {
                logger.error("gRPC error: {}", e.getStatus(), e);
                throw new DataException("Failed to report usage to PaymentGateway due to gRPC error", e);
            }
        } catch (Exception e) {
            logger.error("Failed to report usage to PaymentGateway", e);
            throw new RetriableException("Failed to report usage to PaymentGateway due to an unexpected error", e);
        }
    }
}
