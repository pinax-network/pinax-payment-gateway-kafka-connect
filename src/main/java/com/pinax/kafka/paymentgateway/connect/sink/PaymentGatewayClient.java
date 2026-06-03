package com.pinax.kafka.paymentgateway.connect.sink;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;

import sf.gateway.payment.v1.Gateway.ReportRequest;
import sf.gateway.payment.v1.Gateway.ReportResponse;
import sf.gateway.payment.v1.UsageServiceGrpc;
import sf.gateway.payment.v1.UsageServiceGrpc.UsageServiceBlockingStub;
import sf.metering.v1.MeteringOuterClass.Event;

public class PaymentGatewayClient {
    private final Logger logger = LoggerFactory.getLogger(PaymentGatewayClient.class);

    private final String host;
    private final int port;
    private final boolean useTls;
    private final CallCredentials callCredentials;

    // Package-private (not public) so unit tests in this package can inject a
    // mocked stub without opening a real channel. blockingStub == null is the
    // single source of truth for "not connected".
    ManagedChannel channel;
    UsageServiceBlockingStub blockingStub;

    public PaymentGatewayClient(String endpoint, String token) {
        // The endpoint shape ("scheme://host:port") is already enforced by
        // PaymentGatewaySinkConfigValidator, so this parse cannot fail here.
        URI uri = URI.create(endpoint);
        this.host = uri.getHost();
        this.port = uri.getPort();
        // Honour the scheme: https => TLS, http => plaintext. Previously TLS was
        // always used, so an http:// endpoint silently failed against a
        // plaintext server.
        this.useTls = "https".equalsIgnoreCase(uri.getScheme());

        // Token used to authenticate every request (Bearer credentials).
        this.callCredentials = new BearerToken(token);
    }

    private ManagedChannel createChannel() {
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port);
        if (useTls) {
            try {
                builder.sslContext(GrpcSslContexts.forClient().build());
            } catch (SSLException e) {
                // A broken client TLS setup is a permanent misconfiguration: fail
                // the task fast rather than starting one that can never connect.
                throw new ConnectException("Failed to build client TLS context for " + host + ":" + port, e);
            }
        } else {
            builder.usePlaintext();
        }
        return builder.build();
    }

    /**
     * (Re)builds the gRPC channel and stub. gRPC connects lazily, so this never
     * touches the network — only an invalid TLS setup fails here, and that is a
     * fatal misconfiguration.
     */
    public void start() {
        this.channel = createChannel();
        this.blockingStub = UsageServiceGrpc.newBlockingStub(channel).withCallCredentials(callCredentials);
        logger.info("Started PaymentGateway client for {}://{}:{}", useTls ? "https" : "http", host, port);
    }

    public void stop() {
        if (channel != null) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        // Always clear references so a half-closed channel is never reused; the
        // next report() rebuilds from a clean state.
        channel = null;
        blockingStub = null;
        logger.info("Stopped PaymentGateway client");
    }

    public void report(Collection<SinkRecord> records) {
        // Lazily (re)connect. start() runs once at task start, but a failed batch
        // drops the connection (see PaymentGatewaySinkTask.put), so rebuild it
        // here before the next attempt.
        if (blockingStub == null) {
            start();
        }

        try {
            // Group the metering events by user, sending one ReportRequest per
            // user. Size-based batching is intentionally disabled until
            // StreamingFast supports it (see PaymentGatewaySinkConfig.BATCH_SIZE).
            Map<String, List<Event>> eventsByUser = new HashMap<>();

            for (SinkRecord record : records) {
                if (record.value() == null) {
                    throw new DataException("Record value is null; expected a JSON metering event");
                }

                // Extract the metering event from the record value (JSON).
                Event.Builder eventBuilder = Event.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(record.value().toString(), eventBuilder);
                Event event = eventBuilder.build();

                eventsByUser.computeIfAbsent(event.getUserId(), key -> new ArrayList<>()).add(event);
            }

            for (Map.Entry<String, List<Event>> entry : eventsByUser.entrySet()) {
                ReportRequest reportRequest = ReportRequest.newBuilder()
                        .addAllEvents(entry.getValue())
                        .build();

                logger.info("Reporting {} events for userId {}", entry.getValue().size(), entry.getKey());
                ReportResponse response = blockingStub.report(reportRequest);

                if (response.getRevoked()) {
                    logger.warn("PaymentGateway revoked userId {}: {}", entry.getKey(), response.getRevocationReason());
                }
            }
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to parse metering event from record value", e);
            throw new DataException("Failed to parse metering event", e);
        } catch (StatusRuntimeException e) {
            throw classifyGrpcError(e);
        } catch (DataException e) {
            // Already classified above (e.g. null record value); don't relabel it
            // as a retriable error.
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error while reporting usage to PaymentGateway", e);
            throw new RetriableException("Unexpected error while reporting usage to PaymentGateway", e);
        }
    }

    /**
     * Maps a gRPC status to the right Kafka Connect signal: transient /
     * infrastructure failures become {@link RetriableException} (Connect retries
     * the batch); anything else becomes a non-retriable {@link DataException}.
     */
    private RuntimeException classifyGrpcError(StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();
        logger.error("gRPC error reporting usage: {}", e.getStatus(), e);
        switch (code) {
            case UNAVAILABLE:
            case RESOURCE_EXHAUSTED:
            case INTERNAL:
            case UNKNOWN:
            case UNAUTHENTICATED:
            case PERMISSION_DENIED:
                return new RetriableException("Failed to report usage to PaymentGateway (retriable gRPC error)", e);
            default:
                // INVALID_ARGUMENT, NOT_FOUND, etc. — retrying cannot help.
                return new DataException("Failed to report usage to PaymentGateway (non-retriable gRPC error)", e);
        }
    }
}
