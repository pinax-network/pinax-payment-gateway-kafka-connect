package com.pinax.kafka.paymentgateway.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import sf.gateway.payment.v1.Gateway.ReportRequest;
import sf.gateway.payment.v1.Gateway.ReportResponse;
import sf.gateway.payment.v1.UsageServiceGrpc.UsageServiceBlockingStub;

/**
 * Unit tests for {@link PaymentGatewayClient}. A mocked blocking stub is injected
 * into the package-private {@code blockingStub} field so {@code report()} never
 * opens a real channel.
 */
class PaymentGatewayClientTest {

    private UsageServiceBlockingStub stub;
    private PaymentGatewayClient client;

    @BeforeEach
    void setUp() {
        stub = mock(UsageServiceBlockingStub.class);
        when(stub.report(any(ReportRequest.class))).thenReturn(ReportResponse.getDefaultInstance());

        client = new PaymentGatewayClient("gateway.test:443", "token", false, false);
        client.blockingStub = stub; // inject; report() skips start() when already set
    }

    private static SinkRecord record(String value) {
        return new SinkRecord("metering", 0, null, "k", null, value, 0L);
    }

    @Test
    void report_groupsEventsByUser() {
        client.report(List.of(
                record("{\"userId\":\"u1\"}"),
                record("{\"userId\":\"u1\"}"),
                record("{\"userId\":\"u2\"}")));

        // One ReportRequest per distinct user; 3 events in total across them.
        ArgumentCaptor<ReportRequest> captor = ArgumentCaptor.forClass(ReportRequest.class);
        verify(stub, times(2)).report(captor.capture());
        int totalEvents = captor.getAllValues().stream().mapToInt(ReportRequest::getEventsCount).sum();
        assertEquals(3, totalEvents);
    }

    @Test
    void report_nullRecordValue_throwsDataException() {
        SinkRecord nullValue = new SinkRecord("metering", 0, null, "k", null, null, 0L);
        assertThrows(DataException.class, () -> client.report(List.of(nullValue)));
    }

    @Test
    void report_malformedJson_throwsDataException() {
        assertThrows(DataException.class, () -> client.report(List.of(record("this is not json"))));
    }

    @Test
    void report_retriableGrpcStatus_throwsRetriable() {
        when(stub.report(any(ReportRequest.class))).thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
        assertThrows(RetriableException.class, () -> client.report(List.of(record("{\"userId\":\"u1\"}"))));
    }

    @Test
    void report_nonRetriableGrpcStatus_throwsDataException() {
        when(stub.report(any(ReportRequest.class))).thenThrow(new StatusRuntimeException(Status.INVALID_ARGUMENT));
        assertThrows(DataException.class, () -> client.report(List.of(record("{\"userId\":\"u1\"}"))));
    }

    @Test
    void report_revokedResponse_doesNotThrow() {
        when(stub.report(any(ReportRequest.class)))
                .thenReturn(ReportResponse.newBuilder().setRevoked(true).setRevocationReason("over quota").build());
        // A revocation is logged as a warning, not treated as a failure.
        client.report(List.of(record("{\"userId\":\"u1\"}")));
        verify(stub).report(any(ReportRequest.class));
    }

    @Test
    void startThenStop_tlsDefault_buildsChannelAndClears() {
        PaymentGatewayClient c = new PaymentGatewayClient("gateway.test:443", "token", false, false);
        c.start();
        assertNotNull(c.channel);
        assertNotNull(c.blockingStub);

        c.stop();
        assertNull(c.channel);
        assertNull(c.blockingStub);
    }

    @Test
    void startThenStop_plaintext_buildsChannelAndClears() {
        PaymentGatewayClient c = new PaymentGatewayClient("localhost:8080", "token", true, false);
        c.start();
        assertNotNull(c.channel);

        c.stop();
        assertNull(c.channel);
    }

    @Test
    void startThenStop_insecureTls_buildsChannelAndClears() {
        PaymentGatewayClient c = new PaymentGatewayClient("gateway.test:443", "token", false, true);
        c.start(); // builds a TLS channel with an insecure trust manager
        assertNotNull(c.channel);

        c.stop();
        assertNull(c.channel);
    }

    @Test
    void constructor_plaintextAndInsecure_throwsConfigException() {
        assertThrows(ConfigException.class,
                () -> new PaymentGatewayClient("gateway.test:443", "token", true, true));
    }

    @Test
    void stop_withoutStart_isNoOp() {
        PaymentGatewayClient c = new PaymentGatewayClient("localhost:8080", "token", true, false);
        c.stop(); // channel/stub never created — must not throw
        assertNull(c.channel);
    }
}
