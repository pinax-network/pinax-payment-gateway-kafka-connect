package com.pinax.kafka.paymentgateway.connect.sink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

/**
 * Unit tests for {@link PaymentGatewaySinkTask}. The task constructs its
 * {@link PaymentGatewayClient} in {@code start()}, so we intercept that
 * construction with Mockito — no real gRPC channel is ever opened.
 */
class PaymentGatewaySinkTaskTest {

    private static Map<String, String> props() {
        Map<String, String> props = new HashMap<>();
        props.put(PaymentGatewaySinkConfig.ENDPOINT, "gateway.test:443");
        props.put(PaymentGatewaySinkConfig.TOKEN, "secret");
        return props;
    }

    private static SinkRecord record() {
        return new SinkRecord("metering", 0, null, "k", null, "{\"userId\":\"u1\"}", 0L);
    }

    @Test
    void put_reportSucceeds_doesNotStopClient() {
        try (MockedConstruction<PaymentGatewayClient> mocked = mockConstruction(PaymentGatewayClient.class)) {
            PaymentGatewaySinkTask task = new PaymentGatewaySinkTask();
            task.start(props());

            task.put(List.of(record()));

            PaymentGatewayClient client = mocked.constructed().get(0);
            verify(client).report(anyCollection());
            verify(client, never()).stop();
        }
    }

    @Test
    void put_reportFails_stopsClientAndRethrows() {
        try (MockedConstruction<PaymentGatewayClient> mocked = mockConstruction(PaymentGatewayClient.class,
                (m, ctx) -> doThrow(new RetriableException("boom")).when(m).report(anyCollection()))) {
            PaymentGatewaySinkTask task = new PaymentGatewaySinkTask();
            task.start(props());

            assertThrows(RetriableException.class, () -> task.put(List.of(record())));

            verify(mocked.constructed().get(0)).stop();
        }
    }

    @Test
    void put_emptyRecords_isNoOp() {
        try (MockedConstruction<PaymentGatewayClient> mocked = mockConstruction(PaymentGatewayClient.class)) {
            PaymentGatewaySinkTask task = new PaymentGatewaySinkTask();
            task.start(props());

            assertDoesNotThrow(() -> task.put(Collections.emptyList()));

            verify(mocked.constructed().get(0), never()).report(anyCollection());
        }
    }

    @Test
    void stop_beforeStart_isNoOp() {
        PaymentGatewaySinkTask task = new PaymentGatewaySinkTask();
        assertDoesNotThrow(task::stop);
    }
}
